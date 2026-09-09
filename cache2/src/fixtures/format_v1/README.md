# Format 1 golden fixtures

These fixtures pin the version 1 on-disk bytes. Changes require an explicit format-version decision; tests never regenerate them.

The sparse representation starts with the complete byte length. Each following line contains a hexadecimal offset and hexadecimal bytes; unspecified bytes are zero.
