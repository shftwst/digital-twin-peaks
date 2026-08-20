# Development and conformance

This repository currently implements the first platform-contract proving slice:
Identity, CRM, Event Relay, Control, deterministic scenarios, and a test-only
webhook receiver. The [approved estate design](superpowers/specs/2026-08-19-enterprise-digital-twins-design.md)
defines the wider refund estate. The [future-twin specification guide](superpowers/specs/2026-08-19-future-twin-specification-guide.md)
defines how to specify later systems and workflows.

The numbered technical plans all build refund Release 1; they are not separate
business releases. Supplier onboarding is the second future business release.

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

Compose writes the credential-free, versioned endpoint manifest to
`artifacts/endpoints/manifest-v1.json` before Identity or CRM starts. Its exact
schema is:

```json
{
  "schemaVersion": "1",
  "services": {
    "identity": {
      "containerUrl": "http://identity:8000",
      "loopbackUrl": "http://127.0.0.1:8101"
    },
    "crm": {
      "containerUrl": "http://crm:8000",
      "loopbackUrl": "http://127.0.0.1:8102"
    }
  }
}
```

A host SUT reads this file directly. A container SUT should mount
`./artifacts/endpoints` read-only and select each `containerUrl`. The manifest
intentionally excludes private Control, Relay, database, and admin endpoints.
Identity discovery supports exactly the two configured Identity origins. A
request to the container origin receives container discovery URLs and a token
with the container issuer; a request to the loopback origin receives loopback
URLs and a matching issuer. Unknown or forwarded authorities are not trusted.

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

## Event-plane health

Identity and CRM remain non-ready until their supervised outbox dispatcher is
running and the Relay reports the same active scenario epoch. The Relay worker
updates a Relay-owned operational heartbeat using wall time. Relay readiness
fails when that heartbeat is missing or stale. The heartbeat is operational
health, not scenario business state, so reset does not clear it. The worker has
no HTTP listener; its Compose health check reads only this persisted heartbeat.

Long-running twin processes use an `on-failure` restart policy. Startup remains
acyclic: the worker waits for PostgreSQL and Control, the Relay API waits for a
healthy worker, and source services then wait for a healthy Relay API.

## Fault capability extension

Control accepts a fault rule only when its phase/effect pair is valid and its
exact target, operation, phase, and effect appear in
`enterprise_twins.common.control.fault_capabilities.FAULT_CAPABILITIES`. The
current registry contains only implemented probes for Identity token issue,
CRM note creation after commit, and Relay webhook delivery. To add a future
twin fault, implement and test the probe and effect handler first, then add the
smallest matching registry entry. Unsupported combinations return the common
422 envelope and are never stored or activated.

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
is not attached to `twin-control`. A public-only probe must neither resolve nor
connect to Control, PostgreSQL, every Relay process, or any admin process. The
conformance container receives no database URL and has no Docker socket. The
canonical network evidence covers all 13 long-running Plan 1 services, exact
internal-network flags, runtime memberships, and exact loopback bindings. The
host wrapper performs the CRM restart and reads Docker metadata.

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
