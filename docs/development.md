# Development and conformance

This repository currently implements the first platform-contract proving slice:
Identity, CRM, Event Relay, Control, deterministic scenarios, and a test-only
webhook receiver. The [approved estate design](superpowers/specs/2026-08-19-enterprise-digital-twins-design.md)
defines the wider refund estate. The [future-twin specification guide](superpowers/specs/2026-08-19-future-twin-specification-guide.md)
defines how to specify later systems and workflows.

## Start and control the estate

Run these commands from the repository root:

```text
uv sync --locked --all-groups
docker compose up -d --build --wait
docker compose exec control twins status
docker compose exec control twins reset platform-contracts --version 1 --random-seed 7
docker compose exec control twins time advance PT5M
./scripts/conformance platform-contracts
docker compose logs --since 5m control identity crm event-relay-api event-relay-worker
docker compose down
```

The host can reach only the business APIs:

| Service | Host URL | Container URL |
|---|---|---|
| Identity | `http://127.0.0.1:8101` | `http://identity:8000` |
| CRM | `http://127.0.0.1:8102` | `http://crm:8000` |

Control, Relay, PostgreSQL, admin processes, and the conformance receiver have
no host port. All credentials and records are synthetic and local to this test
estate. Do not reuse the example credentials outside this repository.

## Reset and virtual time

Reset reloads a named scenario into running containers. It does not rebuild or
restart the estate. A successful reset returns its resolved random seed, a new
scenario epoch, participant reports, and a manifest checksum. Repeating the
same scenario version and seed produces the same checksum and a different
epoch. Reset clears business writes, idempotency records, subscriptions,
pending deliveries, and fault rules.

Control owns business time. Use `twins time advance` or the private Control API
for scheduled transitions. Polling a due operation does not advance virtual
time. Host time is used only for process operation and conformance deadlines.

## Conformance commands

The commands below call the public Identity and CRM APIs manually in sequence.
They use Control only for reset, time, faults, and diagnostics.

```text
./scripts/conformance platform-success
./scripts/conformance platform-failure
./scripts/conformance platform-contracts
```

`platform-success` proves authentication, exact customer matching, optimistic
concurrency, append-only note creation, idempotent replay, signed webhooks, and
the deterministic Event Relay retry. `platform-failure` proves denial,
ambiguous search, stale writes, idempotency mismatch, target rejection, a
response timeout after commit, reconciliation, and reset. `platform-contracts`
runs both sequences, restarts CRM, checks state survival, and checks the Docker
network boundary.

Each invocation creates `artifacts/platform-contracts/runs/<run-id>/`. A passed
run publishes `artifacts/platform-contracts/latest-run.json` only after all
selected assertions pass. The run directory contains redacted ordered HTTP
transcripts, webhook attempts, fault activations, reset evidence, restart
metadata, and network evidence. It contains no bearer tokens, client secrets,
webhook signing secrets, unrestricted headers, or raw sensitive bodies.

The opt-in wrapper acceptance test can start, restart, and reset the Docker
estate. Run it explicitly:

```text
RUN_PLATFORM_CONFORMANCE=1 uv run pytest tests/conformance/test_platform_script.py -q
```

An ordinary `uv run pytest -q` does not start Docker.

## Network boundary

| Network | Members in this slice | Purpose |
|---|---|---|
| `twin-public` | Identity, CRM, test runner, conformance driver, public probe | Business API access |
| `twin-integration` | PostgreSQL, Identity, CRM, Relay API and worker | Permitted internal transport |
| `twin-webhook-egress` | Relay worker, webhook receiver | Callback delivery only |
| `twin-control` | Control, twin admin processes, twins, test runner, conformance driver | Private reset, time, faults, and diagnostics |
| `conformance-admin` | Conformance driver, webhook receiver | Test-only receiver administration |

The conformance driver is not attached to `twin-webhook-egress`. The receiver
is not attached to `twin-control`. A public-only probe must not resolve
Control. The conformance container receives no database URL and has no Docker
socket. The host wrapper performs the CRM restart and reads Docker metadata.

## Current acceptance boundary

This slice proves the common platform contracts with Identity and CRM. It does
not implement the remaining refund systems or claim refund-workflow
acceptance. Workflow decisions, ordering, human tasks, policy evaluation, and
case state remain responsibilities of the external SUT.

This slice does not implement or export OpenTelemetry traces. Its artefacts
prove the named HTTP, event, reset, restart, fault, and network assertions, but
do not claim the full estate observability criteria in the approved design.

## Specify future twins

Use the [future-twin specification guide](superpowers/specs/2026-08-19-future-twin-specification-guide.md)
with these five inputs:

1. The future scenario document.
2. The release boundary, if already known.
3. Any named systems or protocols that require exact compatibility.
4. Deployment, language, regulatory, or data-location constraints.
5. The existing twin-platform specification and current contracts.

Invoke the guide with this instruction:

```text
Read the future enterprise-twin specification guide and the supplied workflow
scenario. Separate the human workflow from the surrounding systems. Research
the relevant enterprise environment using current primary sources. Propose the
smallest useful release, map the independent systems and their APIs, define
stateful success and failure behaviour, and produce the specification described
by the guide. Do not assign workflow decisions or orchestration to a twin.
```
