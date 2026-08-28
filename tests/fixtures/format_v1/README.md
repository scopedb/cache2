# Format 1 golden fixtures

These files are committed byte truth for the initial recovery and record formats.
The sparse text representation starts with the complete byte length; every
following line gives a hexadecimal offset and non-zero bytes, and unspecified
bytes are zero.

The fixtures bind one 128-Region, 32 MiB-per-Region cache with asymmetric
identities and counters:

- data generation `7`, cache UUID bytes `01`, data identity bytes `02`;
- key hash algorithm `1` (seeded XXH3-64);
- hash seed `0x123456789abcdef0` and config fingerprint
  `0x8877665544332211`;
- CLEAN state generation `19`, image identity bytes `03`, image generation
  `11`;
- 8,064 index slots, a 64 KiB index image, and one 4 KiB Region metadata page;
- index pages hold 504 stable 8-byte slots each.

An intentional change to these bytes requires an explicit format-version decision.
Tests never regenerate the fixtures.

The committed set covers a value record, the recovery control pages, one
complete 504-slot index page with one value, and a separate three-page Region
metadata image containing four Regions and two index partitions.
