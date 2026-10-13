# Op-Integrations

Canonical Optiply repository for integration source code: Singer taps, Singer targets, ETLs, mapping docs, and shared helper code.

Morph maintains this repo together with [`Optiply/morph-v2`](https://github.com/Optiply/morph-v2):

- `morph-v2` is the runtime/policy/agent workspace.
- `Op-Integrations` is where reusable integration implementation work lives.

## Repository layout

```text
taps/          Singer taps that extract from source systems
targets/       Singer targets that load to Optiply or integration-side APIs
etl/           HotGlue/Optiply ETL notebooks and transform code
docs/          API notes, mapping docs, setup notes, and troubleshooting
shared/        Shared utilities used by multiple taps/targets/ETLs
scripts/       Repo maintenance and scaffolding scripts
```

Every integration should use the same slug in each area:

```text
taps/tap-<slug>/
targets/target-<slug>/
etl/<slug>/
docs/<slug>/
```

Example: `extend` uses `taps/tap-extend/`, `etl/extend/`, and `docs/extend/`.

## Integration inventory

| Integration | Status in repo | Tap | Target | ETL | Docs |
|---|---|---|---|---|---|
| Colleqtive | Tap scaffold/implementation | [`tap-colleqtive`](taps/tap-colleqtive/) | — | — | — |
| Extend Commerce / Lxir | Tap + ETL + mapping docs | [`tap-extend`](taps/tap-extend/) | external/none in repo | [`etl/extend`](etl/extend/) | [`docs/extend`](docs/extend/) |
| Fathom | Tap implementation | [`tap-fathom`](taps/tap-fathom/) | — | — | — |
| HotGlue Platform API | Tap implementation | [`tap-hotglue`](taps/tap-hotglue/) | — | — | — |
| Personio (HR, internal use) | Tap implementation | [`tap-personio`](taps/tap-personio/) | — | — | — |

Use this table as the source of truth for what has landed in this repo. If a tap, target, ETL, or doc is still external, link it or say `external`; do not leave readers guessing.

## What belongs here

Put these in this repo:

- Singer taps and targets used by Optiply integrations.
- HotGlue ETLs, notebooks, and transform helper modules.
- Connector-specific mapping docs and source API notes.
- Reusable integration helpers that are safe to share across connectors.
- Smoke-test configs with fake/example values only.

Do **not** commit:

- tenant credentials, HotGlue tokens, OAuth refresh/access tokens, API keys, or real Postman environments
- local sync output, snapshots, downloaded customer payloads, or generated cache files
- one-off production investigation dumps that contain customer data

`.gitignore` already excludes common local output paths such as `sync-output/`, `etl-output/`, `snapshots/`, `config.json`, `.venv/`, and notebook checkpoints.

## New integration workflow

Create a consistent scaffold:

```bash
./scripts/new-integration.sh "Source Name" "E-commerce" "API Key" "https://vendor.example/docs"
```

That creates:

```text
taps/tap-<slug>/
targets/target-<slug>/
etl/<slug>/
docs/<slug>/
```

If you also want an Obsidian/vault project page, set `OP_INTEGRATIONS_VAULT` before running the script. The repo itself should not depend on a local vault path.

## Development standards

### Taps

- Prefer `hotglue-singer-sdk` unless a legacy tap already uses Meltano `singer-sdk`.
- Keep auth, pagination, rate-limit, and incremental cursor behavior explicit in code and README.
- Include `config.json.example` with fake values only.
- Add at least a discover/import smoke test or a `tests/test_tap.py` fixture.

### Targets

- Targets should be idempotent where the destination API allows it.
- Document write semantics clearly: create/update/upsert, delete handling, and retry behavior.
- Never run a target against production/customer data without explicit operator approval.

### ETLs

- Keep notebooks deterministic: same inputs should produce the same output files.
- Put reusable transform helpers in `etl/<slug>/utils/` or `shared/` if multiple connectors need them.
- Document source-field to Optiply-field mappings in `docs/<slug>/data-mapping.md`.

### Docs

- Each integration doc folder should include, at minimum:
  - `setup.md` or an equivalent setup section
  - `data-mapping.md` for field mappings when Optiply entities are produced
  - `troubleshooting.md` once operational failure modes are known

## Local checks

There is no single full-suite runner yet. Before pushing broad maintenance changes, run the relevant checks:

```bash
# Shell scripts
bash -n scripts/*.sh

# Python syntax for checked-in connector code
python3 -m py_compile $(find taps etl shared -name '*.py')

# For an individual tap, install and run its own smoke commands, for example:
cd taps/tap-fathom
python3 -m venv /tmp/tap-fathom-venv
/tmp/tap-fathom-venv/bin/python -m pip install -e .
/tmp/tap-fathom-venv/bin/tap-fathom --config config.json --discover
```

Use fake config files for local docs/smoke examples. Real tenant config belongs in HotGlue or the operator-controlled runtime environment, not Git.
