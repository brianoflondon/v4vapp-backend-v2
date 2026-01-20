# Log rotation naming and `rotation_folder` option

## Summary

This document describes the new log rotation filename behavior implemented in
`v4vapp-backend-v2`.

## What changed

- Rotated files now have the rotation index placed _before_ the final file extension
  (e.g. `hive_monitor_v2.001.jsonl` instead of `hive_monitor_v2.jsonl.1`).
- Rotation indices are zero-padded. The padding width is the greater of 3 and the
  number of digits in the handler's `backupCount`. Examples:
  - `...jsonl.1` -> `...001.jsonl`
  - `...jsonl.10` -> `...010.jsonl`
- Optional `rotation_folder` config (default: `false`) moves rotated files into a
  `rotation/` subdirectory next to the log file (e.g. `logs/rotation/hive_monitor_v2.001.jsonl`).

## Configuration

Add to your YAML logging config under `logging`:

```yaml
logging:
  ...
  rotation_folder: true   # or false (default)
```

## Notes for operators

- The change is applied by a custom `namer` attached to `RotatingFileHandler` during
  startup in `src/v4vapp_backend_v2/config/setup.py`.
- The namer will create the `rotation/` folder if `rotation_folder: true`.
- Default padding width is 3 digits. If you prefer a different width, we can add
  a config option for `rotation_padding_width`.

## Developer notes

- Implementation: `make_rotation_namer` in `src/v4vapp_backend_v2/config/setup.py`.
- Unit tests: `tests/mylogger/test_rotating_namer.py`.

## Examples

**Before (old):**

```
hive_monitor_v2.jsonl
hive_monitor_v2.jsonl.1
hive_monitor_v2.jsonl.2
```

**After (new):**

```
hive_monitor_v2.jsonl
rotation/hive_monitor_v2.001.jsonl   # if rotation_folder: true
hive_monitor_v2.002.jsonl            # if rotation_folder: false
```
