# Dash seed files

`*.mnemonic` is gitignored. Copy the `.example` next to it and put **only that network's** BIP39 words in the real file.

Format (UTF-8):

- 12 or 24 words, spaces between them
- `#` lines and blank lines are ignored
- no quotes, no commas, no `mnemonic:` prefix

Paths in YAML are relative to the process cwd (`/` of the repo locally, `/app` in compose):

```yaml
mnemonic_file: config/secrets/dash-testnet.mnemonic
```

`payouts_enabled: true` is required before `POST /v2/dash/payouts` will sign.
