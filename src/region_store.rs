//! Recovery and shutdown state machine for the Region-backed cache.
//!
//! The coordinator owns no file-format or data-plane logic. A
//! [`RegionBackend`] supplies those operations, while this module enforces the
//! order that makes warm recovery safe:
//!
//! - inspect recovery before constructing an index;
//! - publish `RUNNING` before exposing a runtime;
//! - publish `CLEAN` only after freezing and persisting a complete image;
//! - release exclusive ownership on every terminal path.

use std::io;

use crate::index::MAX_INDEX_SLOTS;
use crate::snapshot::StartupMode;

/// Result of inspecting the latest valid state record.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum RecoveryPlan<T> {
    Fresh,
    Running,
    Clean(T),
}

/// Physical lifecycle operations required by [`RegionStore`].
pub(crate) trait RegionBackend {
    type Runtime;
    type CleanImage;
    type FrozenView;
    type PreparedClean;

    /// Acquire exclusive ownership of all files before inspection.
    fn acquire_exclusive(&mut self) -> io::Result<()>;

    /// This must not allocate or scan the full index or Region data extents.
    fn inspect_recovery(
        &mut self,
        index_slots: usize,
    ) -> io::Result<RecoveryPlan<Self::CleanImage>>;

    /// Construct a provisional empty runtime without starting workers.
    fn anonymous_runtime(&mut self, index_slots: usize) -> io::Result<Self::Runtime>;

    /// `Ok(None)` rejects the complete image and selects a cold start.
    fn map_clean_runtime(
        &mut self,
        clean: Self::CleanImage,
        index_slots: usize,
    ) -> io::Result<Option<Self::Runtime>>;

    /// Publish `RUNNING` durably before the runtime can be observed. The
    /// concrete backend replaces both state slots so a torn page cannot revive
    /// a previous `CLEAN` generation after Region reuse starts.
    fn publish_running(&mut self) -> io::Result<()>;

    /// Start workers only after `RUNNING`; an error must tear them down.
    fn start_runtime(&mut self, runtime: Self::Runtime) -> io::Result<Self::Runtime>;

    /// Quiesce all mutation sources without constructing recovery metadata.
    fn stop_fast(&mut self, runtime: Self::Runtime) -> io::Result<()>;

    /// Quiesce the runtime and return its immutable recovery authority.
    fn freeze_warm(&mut self, runtime: Self::Runtime) -> io::Result<Self::FrozenView>;

    /// Make completed data and one complete image durable.
    fn persist_frozen(&mut self, view: &Self::FrozenView) -> io::Result<Self::PreparedClean>;

    /// Publish `CLEAN` durably using the token returned after persistence.
    fn publish_clean(&mut self, prepared: Self::PreparedClean) -> io::Result<()>;

    /// Release ownership, including during error unwinding after acquisition.
    fn release_exclusive(&mut self) -> io::Result<()>;
}

/// Owns the exclusive lifecycle of one backend runtime.
pub(crate) struct RegionStore<B: RegionBackend> {
    backend: B,
    runtime: Option<B::Runtime>,
    startup: StartupMode,
    closed: bool,
}

impl<B: RegionBackend> RegionStore<B> {
    pub(crate) fn open(index_slots: usize, mut backend: B) -> io::Result<Self> {
        validate_index_slots(index_slots)?;
        backend.acquire_exclusive()?;

        let opened = (|| {
            let plan = backend.inspect_recovery(index_slots)?;
            let (runtime, startup) = match plan {
                RecoveryPlan::Fresh => {
                    (backend.anonymous_runtime(index_slots)?, StartupMode::Fresh)
                }
                RecoveryPlan::Running => (
                    backend.anonymous_runtime(index_slots)?,
                    StartupMode::ColdAfterUncleanShutdown,
                ),
                RecoveryPlan::Clean(clean) => {
                    match backend.map_clean_runtime(clean, index_slots)? {
                        Some(runtime) => (runtime, StartupMode::Warm),
                        None => (
                            backend.anonymous_runtime(index_slots)?,
                            StartupMode::ColdAfterRejectedImage,
                        ),
                    }
                }
            };

            backend.publish_running()?;
            let runtime = backend.start_runtime(runtime)?;
            Ok((runtime, startup))
        })();

        match opened {
            Ok((runtime, startup)) => Ok(Self {
                backend,
                runtime: Some(runtime),
                startup,
                closed: false,
            }),
            Err(error) => {
                let _ = backend.release_exclusive();
                Err(error)
            }
        }
    }

    pub(crate) const fn startup(&self) -> StartupMode {
        self.startup
    }

    pub(crate) fn runtime(&self) -> io::Result<&B::Runtime> {
        if self.closed {
            return Err(closed_error());
        }
        self.runtime.as_ref().ok_or_else(closed_error)
    }

    #[cfg(test)]
    pub(crate) fn runtime_mut(&mut self) -> io::Result<&mut B::Runtime> {
        if self.closed {
            return Err(closed_error());
        }
        self.runtime.as_mut().ok_or_else(closed_error)
    }

    /// Stop without producing a recovery image. The next open starts empty.
    pub(crate) fn close_fast(&mut self) -> io::Result<()> {
        self.close(false)
    }

    /// Freeze and publish one complete warm-restart image.
    pub(crate) fn close_warm(&mut self) -> io::Result<()> {
        self.close(true)
    }

    fn close(&mut self, warm: bool) -> io::Result<()> {
        if self.closed {
            return Ok(());
        }

        let result = match self.runtime.take() {
            Some(runtime) if warm => self
                .backend
                .freeze_warm(runtime)
                .and_then(|frozen| self.backend.persist_frozen(&frozen))
                .and_then(|prepared| self.backend.publish_clean(prepared)),
            Some(runtime) => self.backend.stop_fast(runtime),
            None => Err(closed_error()),
        };

        let unlock = self.backend.release_exclusive();
        self.closed = true;
        result.and(unlock)
    }
}

impl<B: RegionBackend> Drop for RegionStore<B> {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.close_fast();
        }
    }
}

fn closed_error() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "RegionStore is closed")
}

fn validate_index_slots(index_slots: usize) -> io::Result<()> {
    if !(8..=MAX_INDEX_SLOTS).contains(&index_slots) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "RegionStore index slots must be in 8..=536870912",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Event {
        Lock,
        Inspect,
        Anonymous,
        Map,
        Running,
        Start,
        StopFast,
        Freeze,
        Persist,
        Clean,
        Unlock,
    }

    #[derive(Clone, Copy)]
    enum Plan {
        Fresh,
        Running,
        Clean,
        RejectedClean,
    }

    struct Backend {
        plan: Plan,
        events: Rc<RefCell<Vec<Event>>>,
        fail_at: Option<Event>,
    }

    impl Backend {
        fn record(&self, event: Event) -> io::Result<()> {
            self.events.borrow_mut().push(event);
            if self.fail_at == Some(event) {
                Err(io::Error::other(format!("{event:?}")))
            } else {
                Ok(())
            }
        }
    }

    impl RegionBackend for Backend {
        type Runtime = usize;
        type CleanImage = bool;
        type FrozenView = usize;
        type PreparedClean = ();

        fn acquire_exclusive(&mut self) -> io::Result<()> {
            self.record(Event::Lock)
        }

        fn inspect_recovery(
            &mut self,
            _index_slots: usize,
        ) -> io::Result<RecoveryPlan<Self::CleanImage>> {
            self.record(Event::Inspect)?;
            Ok(match self.plan {
                Plan::Fresh => RecoveryPlan::Fresh,
                Plan::Running => RecoveryPlan::Running,
                Plan::Clean => RecoveryPlan::Clean(true),
                Plan::RejectedClean => RecoveryPlan::Clean(false),
            })
        }

        fn anonymous_runtime(&mut self, index_slots: usize) -> io::Result<Self::Runtime> {
            self.record(Event::Anonymous)?;
            Ok(index_slots)
        }

        fn map_clean_runtime(
            &mut self,
            clean: Self::CleanImage,
            index_slots: usize,
        ) -> io::Result<Option<Self::Runtime>> {
            self.record(Event::Map)?;
            Ok(clean.then_some(index_slots))
        }

        fn publish_running(&mut self) -> io::Result<()> {
            self.record(Event::Running)
        }

        fn start_runtime(&mut self, runtime: Self::Runtime) -> io::Result<Self::Runtime> {
            self.record(Event::Start)?;
            Ok(runtime)
        }

        fn stop_fast(&mut self, _runtime: Self::Runtime) -> io::Result<()> {
            self.record(Event::StopFast)
        }

        fn freeze_warm(&mut self, runtime: Self::Runtime) -> io::Result<Self::FrozenView> {
            self.record(Event::Freeze)?;
            Ok(runtime)
        }

        fn persist_frozen(&mut self, _view: &Self::FrozenView) -> io::Result<Self::PreparedClean> {
            self.record(Event::Persist)
        }

        fn publish_clean(&mut self, _prepared: Self::PreparedClean) -> io::Result<()> {
            self.record(Event::Clean)
        }

        fn release_exclusive(&mut self) -> io::Result<()> {
            self.record(Event::Unlock)
        }
    }

    fn backend(plan: Plan, fail_at: Option<Event>) -> (Backend, Rc<RefCell<Vec<Event>>>) {
        let events = Rc::new(RefCell::new(Vec::new()));
        (
            Backend {
                plan,
                events: Rc::clone(&events),
                fail_at,
            },
            events,
        )
    }

    #[test]
    fn invalid_capacity_is_rejected_before_ownership_or_allocation() {
        for index_slots in [0, 1, 7, MAX_INDEX_SLOTS + 1] {
            let (backend, events) = backend(Plan::Fresh, None);
            assert!(RegionStore::open(index_slots, backend).is_err());
            assert!(events.borrow().is_empty());
        }
    }

    #[test]
    fn recovery_plan_selects_one_runtime_before_the_running_barrier() {
        for (plan, startup, expected) in [
            (
                Plan::Fresh,
                StartupMode::Fresh,
                vec![
                    Event::Lock,
                    Event::Inspect,
                    Event::Anonymous,
                    Event::Running,
                    Event::Start,
                ],
            ),
            (
                Plan::Running,
                StartupMode::ColdAfterUncleanShutdown,
                vec![
                    Event::Lock,
                    Event::Inspect,
                    Event::Anonymous,
                    Event::Running,
                    Event::Start,
                ],
            ),
            (
                Plan::Clean,
                StartupMode::Warm,
                vec![
                    Event::Lock,
                    Event::Inspect,
                    Event::Map,
                    Event::Running,
                    Event::Start,
                ],
            ),
            (
                Plan::RejectedClean,
                StartupMode::ColdAfterRejectedImage,
                vec![
                    Event::Lock,
                    Event::Inspect,
                    Event::Map,
                    Event::Anonymous,
                    Event::Running,
                    Event::Start,
                ],
            ),
        ] {
            let (backend, events) = backend(plan, None);
            let mut store = RegionStore::open(8, backend).unwrap();
            assert_eq!(store.startup(), startup);
            assert_eq!(*events.borrow(), expected);
            store.close_fast().unwrap();
        }
    }

    #[test]
    fn shutdown_modes_are_disjoint_and_release_ownership() {
        for (warm, expected) in [
            (false, vec![Event::StopFast, Event::Unlock]),
            (
                true,
                vec![Event::Freeze, Event::Persist, Event::Clean, Event::Unlock],
            ),
        ] {
            let (backend, events) = backend(Plan::Fresh, None);
            let mut store = RegionStore::open(8, backend).unwrap();
            events.borrow_mut().clear();
            if warm {
                store.close_warm().unwrap();
            } else {
                store.close_fast().unwrap();
            }
            assert_eq!(*events.borrow(), expected);
        }
    }

    #[test]
    fn drop_uses_the_non_recoverable_shutdown_path() {
        let (backend, events) = backend(Plan::Fresh, None);
        let store = RegionStore::open(8, backend).unwrap();
        events.borrow_mut().clear();

        drop(store);

        assert_eq!(*events.borrow(), vec![Event::StopFast, Event::Unlock]);
    }

    #[test]
    fn open_failure_stops_at_the_failed_stage_and_releases_ownership() {
        for (plan, failed, expected) in [
            (
                Plan::Fresh,
                Event::Inspect,
                vec![Event::Lock, Event::Inspect, Event::Unlock],
            ),
            (
                Plan::Fresh,
                Event::Anonymous,
                vec![Event::Lock, Event::Inspect, Event::Anonymous, Event::Unlock],
            ),
            (
                Plan::Clean,
                Event::Map,
                vec![Event::Lock, Event::Inspect, Event::Map, Event::Unlock],
            ),
            (
                Plan::Fresh,
                Event::Running,
                vec![
                    Event::Lock,
                    Event::Inspect,
                    Event::Anonymous,
                    Event::Running,
                    Event::Unlock,
                ],
            ),
            (
                Plan::Fresh,
                Event::Start,
                vec![
                    Event::Lock,
                    Event::Inspect,
                    Event::Anonymous,
                    Event::Running,
                    Event::Start,
                    Event::Unlock,
                ],
            ),
        ] {
            let (backend, events) = backend(plan, Some(failed));
            assert!(RegionStore::open(8, backend).is_err());
            assert_eq!(*events.borrow(), expected, "failed at {failed:?}");
        }
    }

    #[test]
    fn shutdown_failure_does_not_cross_a_publication_boundary() {
        for (warm, failed, expected) in [
            (false, Event::StopFast, vec![Event::StopFast, Event::Unlock]),
            (true, Event::Freeze, vec![Event::Freeze, Event::Unlock]),
            (
                true,
                Event::Persist,
                vec![Event::Freeze, Event::Persist, Event::Unlock],
            ),
            (
                true,
                Event::Clean,
                vec![Event::Freeze, Event::Persist, Event::Clean, Event::Unlock],
            ),
        ] {
            let (backend, events) = backend(Plan::Fresh, Some(failed));
            let mut store = RegionStore::open(8, backend).unwrap();
            events.borrow_mut().clear();
            let result = if warm {
                store.close_warm()
            } else {
                store.close_fast()
            };
            assert!(result.is_err(), "failed at {failed:?}");
            assert_eq!(*events.borrow(), expected, "failed at {failed:?}");
        }
    }
}
