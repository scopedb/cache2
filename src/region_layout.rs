use std::io;

const MAX_REGION_SETS: usize = 64;
const MAX_NAMESPACE_ROUTES: usize = 4096;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Stable identifier for one physical Region partition.
pub struct RegionSetId(u16);

impl RegionSetId {
    /// Creates an identifier. Zero is the implicit single-set default.
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Returns the underlying identifier.
    pub const fn get(self) -> u16 {
        self.0
    }
}

impl From<u16> for RegionSetId {
    fn from(value: u16) -> Self {
        Self::new(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Static capacity configuration for one RegionSet.
///
/// Weights divide the fixed number of Regions. They do not resize the cache at
/// runtime and do not control how many append shards the set receives.
pub struct RegionSetConfig {
    id: RegionSetId,
    capacity_weight: u32,
    namespaces: Vec<u32>,
}

impl RegionSetConfig {
    /// Creates a RegionSet with capacity weight one.
    pub fn new(id: impl Into<RegionSetId>) -> Self {
        Self {
            id: id.into(),
            capacity_weight: 1,
            namespaces: Vec::new(),
        }
    }

    /// Sets the relative Region capacity weight. Zero is invalid.
    pub fn with_weight(mut self, weight: u32) -> Self {
        self.capacity_weight = weight;
        self
    }

    /// Routes the listed namespaces to this RegionSet.
    ///
    /// Namespaces not listed by any set use RegionSet zero. A namespace may be
    /// listed by only one set.
    pub fn with_namespaces(mut self, namespaces: impl IntoIterator<Item = u32>) -> Self {
        self.namespaces = namespaces.into_iter().collect();
        self
    }

    /// Returns the stable RegionSet identifier.
    pub const fn id(&self) -> RegionSetId {
        self.id
    }

    /// Returns the relative Region capacity weight.
    pub const fn weight(&self) -> u32 {
        self.capacity_weight
    }

    /// Returns the namespaces explicitly routed to this RegionSet.
    pub fn namespaces(&self) -> &[u32] {
        &self.namespaces
    }
}

/// Resolved physical allocation for one RegionSet.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegionSetAllocation {
    /// Stable RegionSet identifier.
    pub id: RegionSetId,
    /// Physical Region bytes assigned to this set.
    pub capacity_bytes: u64,
    /// Number of fixed-size Regions assigned to this set.
    pub region_count: u32,
    /// Number of ordered append shards assigned to this set.
    pub append_shard_count: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionSetLayout {
    pub(crate) id: RegionSetId,
    pub(crate) first_region: u32,
    pub(crate) region_count: u32,
    pub(crate) first_shard: u32,
    pub(crate) shard_count: u32,
}

impl RegionSetLayout {
    pub(crate) fn contains_region(self, region_id: u32) -> bool {
        region_id >= self.first_region
            && region_id < self.first_region.saturating_add(self.region_count)
    }

    pub(crate) fn contains_shard(self, shard_id: usize) -> bool {
        let Ok(shard_id) = u32::try_from(shard_id) else {
            return false;
        };
        shard_id >= self.first_shard && shard_id < self.first_shard.saturating_add(self.shard_count)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegionLayout {
    sets: Box<[RegionSetLayout]>,
    routes: Box<[(u32, usize)]>,
    region_count: u32,
    shard_count: u32,
}

impl RegionLayout {
    pub(crate) fn build(
        region_count: u32,
        shard_count: u32,
        configured_sets: &[RegionSetConfig],
    ) -> io::Result<Self> {
        let mut configs = if configured_sets.is_empty() {
            vec![RegionSetConfig::new(RegionSetId::default())]
        } else {
            configured_sets.to_vec()
        };
        configs.sort_unstable_by_key(RegionSetConfig::id);
        if configs.len() > MAX_REGION_SETS {
            return Err(invalid_layout("RegionSet count exceeds 64"));
        }
        if configs.len() > shard_count as usize {
            return Err(invalid_layout(
                "RegionSet count cannot exceed the append-shard count",
            ));
        }
        if let Some(config) = configs.iter().find(|config| config.capacity_weight == 0) {
            return Err(invalid_layout(format!(
                "RegionSet {} has zero capacity weight",
                config.id.get()
            )));
        }
        if let Some(pair) = configs.windows(2).find(|pair| pair[0].id == pair[1].id) {
            return Err(invalid_layout(format!(
                "RegionSet id {} is configured more than once",
                pair[0].id.get()
            )));
        }
        if region_count <= shard_count || shard_count == 0 {
            return Err(invalid_layout(
                "Region layout requires at least one shard and one spare Region",
            ));
        }
        configs
            .binary_search_by_key(&RegionSetId::default(), RegionSetConfig::id)
            .map_err(|_| invalid_layout("explicit RegionSet layout must include set zero"))?;

        let total_weight = configs.iter().try_fold(0_u64, |total, config| {
            total.checked_add(u64::from(config.capacity_weight))
        });
        let Some(total_weight) = total_weight.filter(|weight| *weight != 0) else {
            return Err(invalid_layout("RegionSet capacity weight overflow"));
        };

        let mut region_counts = Vec::new();
        region_counts
            .try_reserve_exact(configs.len())
            .map_err(|_| out_of_memory())?;
        let mut remainders = Vec::new();
        remainders
            .try_reserve_exact(configs.len())
            .map_err(|_| out_of_memory())?;
        let mut assigned_regions = 0_u32;
        for config in &configs {
            let weighted = u64::from(region_count)
                .checked_mul(u64::from(config.capacity_weight))
                .ok_or_else(|| invalid_layout("RegionSet capacity calculation overflow"))?;
            let count = u32::try_from(weighted / total_weight)
                .map_err(|_| invalid_layout("RegionSet capacity does not fit u32"))?;
            assigned_regions = assigned_regions
                .checked_add(count)
                .ok_or_else(|| invalid_layout("RegionSet capacity sum overflow"))?;
            region_counts.push(count);
            remainders.push(weighted % total_weight);
        }
        let remaining = region_count
            .checked_sub(assigned_regions)
            .ok_or_else(|| invalid_layout("RegionSet capacity exceeds the data geometry"))?;
        let mut remainder_order: Vec<usize> = (0..configs.len()).collect();
        remainder_order.sort_unstable_by(|left, right| {
            remainders[*right]
                .cmp(&remainders[*left])
                .then_with(|| configs[*left].id.cmp(&configs[*right].id))
        });
        for index in remainder_order.into_iter().take(remaining as usize) {
            region_counts[index] += 1;
        }

        let set_count = u32::try_from(configs.len())
            .map_err(|_| invalid_layout("RegionSet count does not fit u32"))?;
        let shards_per_set = shard_count / set_count;
        let extra_shards = shard_count % set_count;
        let mut sets = Vec::new();
        sets.try_reserve_exact(configs.len())
            .map_err(|_| out_of_memory())?;
        let mut first_region = 0_u32;
        let mut first_shard = 0_u32;
        for (index, config) in configs.iter().enumerate() {
            let shard_count_for_set = shards_per_set + u32::from((index as u32) < extra_shards);
            let region_count_for_set = region_counts[index];
            if region_count_for_set <= shard_count_for_set {
                return Err(invalid_layout(format!(
                    "RegionSet {} receives {region_count_for_set} Regions but needs at least {} for {shard_count_for_set} append shards",
                    config.id.get(),
                    shard_count_for_set + 1
                )));
            }
            sets.push(RegionSetLayout {
                id: config.id,
                first_region,
                region_count: region_count_for_set,
                first_shard,
                shard_count: shard_count_for_set,
            });
            first_region = first_region
                .checked_add(region_count_for_set)
                .ok_or_else(|| invalid_layout("RegionSet region range overflow"))?;
            first_shard = first_shard
                .checked_add(shard_count_for_set)
                .ok_or_else(|| invalid_layout("RegionSet shard range overflow"))?;
        }
        if first_region != region_count || first_shard != shard_count {
            return Err(invalid_layout("RegionSet ranges do not cover the geometry"));
        }

        let route_count = configs.iter().try_fold(0_usize, |total, config| {
            total.checked_add(config.namespaces.len())
        });
        let Some(route_count) = route_count.filter(|count| *count <= MAX_NAMESPACE_ROUTES) else {
            return Err(invalid_layout("too many namespace RegionSet routes"));
        };
        let mut routes = Vec::new();
        routes
            .try_reserve_exact(route_count)
            .map_err(|_| out_of_memory())?;
        for (set_index, config) in configs.iter().enumerate() {
            routes.extend(
                config
                    .namespaces
                    .iter()
                    .map(|namespace_id| (*namespace_id, set_index)),
            );
        }
        routes.sort_unstable_by_key(|route| route.0);
        if let Some(pair) = routes.windows(2).find(|pair| pair[0].0 == pair[1].0) {
            return Err(invalid_layout(format!(
                "namespace {} belongs to more than one RegionSet",
                pair[0].0
            )));
        }
        // Explicit ownership by set zero has the same effective routing as
        // omission. Drop it from runtime lookup and static identity after
        // duplicate declarations have been rejected.
        routes.retain(|route| route.1 != 0);

        Ok(Self {
            sets: sets.into_boxed_slice(),
            routes: routes.into_boxed_slice(),
            region_count,
            shard_count,
        })
    }

    #[cfg(test)]
    pub(crate) fn single(region_count: u32, shard_count: u32) -> io::Result<Self> {
        Self::build(region_count, shard_count, &[])
    }

    #[cfg(test)]
    pub(crate) fn single_unchecked(region_count: u32, shard_count: u32) -> Self {
        Self {
            sets: vec![RegionSetLayout {
                id: RegionSetId::default(),
                first_region: 0,
                region_count,
                first_shard: 0,
                shard_count,
            }]
            .into_boxed_slice(),
            routes: Box::new([]),
            region_count,
            shard_count,
        }
    }

    pub(crate) fn sets(&self) -> &[RegionSetLayout] {
        &self.sets
    }

    pub(crate) fn routes(&self) -> &[(u32, usize)] {
        &self.routes
    }

    pub(crate) fn uses_default_single_set(&self) -> bool {
        self.sets.len() == 1 && self.routes.is_empty()
    }

    pub(crate) fn allocations(&self, region_size: u64) -> io::Result<Vec<RegionSetAllocation>> {
        let mut allocations = Vec::new();
        allocations
            .try_reserve_exact(self.sets.len())
            .map_err(|_| out_of_memory())?;
        for set in &self.sets {
            allocations.push(RegionSetAllocation {
                id: set.id,
                capacity_bytes: u64::from(set.region_count)
                    .checked_mul(region_size)
                    .ok_or_else(|| invalid_layout("RegionSet capacity bytes overflow"))?,
                region_count: set.region_count,
                append_shard_count: set.shard_count,
            });
        }
        Ok(allocations)
    }

    pub(crate) const fn region_count(&self) -> u32 {
        self.region_count
    }

    pub(crate) const fn shard_count(&self) -> u32 {
        self.shard_count
    }

    pub(crate) fn set_index_for_namespace(&self, namespace_id: u32) -> usize {
        self.routes
            .binary_search_by_key(&namespace_id, |route| route.0)
            .map(|index| self.routes[index].1)
            .unwrap_or(0)
    }

    pub(crate) fn set_index_for_shard(&self, shard_id: usize) -> Option<usize> {
        self.sets
            .iter()
            .position(|set| set.contains_shard(shard_id))
    }

    pub(crate) fn set_index_for_region(&self, region_id: u32) -> Option<usize> {
        self.sets
            .iter()
            .position(|set| set.contains_region(region_id))
    }

    pub(crate) fn append_shard(&self, namespace_id: u32, hash: u64) -> usize {
        let set = self.sets[self.set_index_for_namespace(namespace_id)];
        set.first_shard as usize + (hash % u64::from(set.shard_count)) as usize
    }

    pub(crate) fn region_belongs_to_namespace(&self, namespace_id: u32, region_id: u32) -> bool {
        self.sets[self.set_index_for_namespace(namespace_id)].contains_region(region_id)
    }

    pub(crate) fn memory_bytes(&self) -> usize {
        self.sets
            .len()
            .saturating_mul(std::mem::size_of::<RegionSetLayout>())
            .saturating_add(
                self.routes
                    .len()
                    .saturating_mul(std::mem::size_of::<(u32, usize)>()),
            )
    }
}

fn invalid_layout(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn out_of_memory() -> io::Error {
    io::Error::new(
        io::ErrorKind::OutOfMemory,
        "cannot allocate RegionSet layout",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weights_and_shards_form_stable_complete_ranges() {
        let layout = RegionLayout::build(
            101,
            8,
            &[
                RegionSetConfig::new(20).with_weight(9).with_namespaces([9]),
                RegionSetConfig::new(0).with_weight(1).with_namespaces([7]),
            ],
        )
        .unwrap();

        assert_eq!(
            layout.sets(),
            &[
                RegionSetLayout {
                    id: RegionSetId::new(0),
                    first_region: 0,
                    region_count: 10,
                    first_shard: 0,
                    shard_count: 4,
                },
                RegionSetLayout {
                    id: RegionSetId::new(20),
                    first_region: 10,
                    region_count: 91,
                    first_shard: 4,
                    shard_count: 4,
                },
            ]
        );
        assert_eq!(layout.append_shard(7, 6), 2);
        assert_eq!(layout.append_shard(9, 6), 6);
        assert_eq!(layout.append_shard(100, 6), 2);
    }

    #[test]
    fn every_set_requires_an_active_and_spare_region() {
        let error = RegionLayout::build(
            9,
            8,
            &[
                RegionSetConfig::new(0).with_weight(1),
                RegionSetConfig::new(2).with_weight(100),
            ],
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn invalid_namespace_ownership_is_rejected() {
        let cases = [
            vec![RegionSetConfig::new(1)],
            vec![RegionSetConfig::new(0).with_weight(0)],
            vec![
                RegionSetConfig::new(0).with_namespaces([7]),
                RegionSetConfig::new(1).with_namespaces([7]),
            ],
        ];
        for configs in cases {
            let error = RegionLayout::build(8, 2, &configs).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }
    }
}
