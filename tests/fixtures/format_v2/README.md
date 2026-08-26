# Format 2 golden fixtures

Recovery image format 2 binds index pages with 168 stable 24-byte slots per
4 KiB page. The data superblock, state page, and Region record formats remain
at format 1 and continue to use the fixtures in `../format_v1`.

The fixture describes 2,688 index slots, a 64 KiB index image, and one 4 KiB
Region metadata page. Tests never regenerate it.
