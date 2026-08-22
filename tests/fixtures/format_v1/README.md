# Format V1 golden fixtures

These files are committed byte truth, not output regenerated during tests.
They use a sparse text representation: the first line declares the full byte
length, each following line gives a hexadecimal offset and bytes, and all
unspecified bytes are zero.

The fixtures freeze little-endian field offsets, version `1`, CRC32C coverage,
record padding, and all three region states. Values deliberately use asymmetric
numbers:

- clean/dirty superblock generations `0x0102030405060708` /
  `0x0102030405060707`, 32 MiB regions, 7 regions, epoch 3, epoch-start
  seqno 17, next seqno 34, hash `0x1122334455667788`;
- region headers: Free `(id=0)`, Active `(id=1, incarnation=9, seqno=17,
  used=8192)`, Sealed `(id=2, incarnation=7, seqno=8, used=12288)`;
- records: key `key`, value `value`, seqnos 34/35, 96-byte encoded length.
- Hybrid manifest: generation `0x0102030405060708`, cache id bytes `00..0f`,
  version epoch 3/next seqno 35, layout hash `0x1122334455667788`, journal
  generation 9/capacity 64 KiB, checkpoint `(3,34)`, clear floor `(2,99)`;
  its reserved namespace-usage extension is all zero, freezing the legacy V1
  representation that new readers upgrade after one policy-usage scan;
- Hybrid journal: Bucket put for namespace 7, version `(3,35)`, key `key`,
  hash `0x8877665544332211`, bucket 4, 96-byte encoded length.

`superblock_v2.golden` is a complete CRC-valid page with the same common layout
but unsupported version 2; it freezes the reject-without-rewrite fixture.
`hybrid_manifest_v2.golden` provides the equivalent downgrade-rejection fixture
for the Hybrid global manifest.

`cache_deleted.golden` is a complete 40 KiB two-region cache using the default
hash seed. Sealed Region 0 contains `victim=old`; active Region 1 contains the
newer `victim` tombstone and `canary=present`. Superblock A is clean generation
10 and B is dirty generation 9. Opening it must return a victim miss and a
canary hit without rewriting any byte.

Any intentional Format V1 byte change requires an explicit compatibility
decision and fixture review; tests never update these files automatically.
