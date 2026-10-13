# Taps

Singer taps live here under `tap-<slug>/`.

## Naming

```text
taps/tap-<slug>/
  tap_<slug_with_underscores>/
    __init__.py
    tap.py
    streams.py
  config.json.example
  pyproject.toml
  README.md
  tests/
```

## Expectations

- Use fake values in `config.json.example`; never commit real tenant credentials.
- Document auth, pagination, cursor/incremental behavior, and rate limits in the tap README.
- Keep stream names stable once a tap is used in HotGlue.
- Include a minimal discover/import smoke path.

## Current taps

- `tap-colleqtive`
- `tap-extend`
- `tap-fathom`
- `tap-hotglue`
- `tap-personio`
