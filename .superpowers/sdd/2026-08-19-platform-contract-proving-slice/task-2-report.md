# Task 2 report: common HTTP contract

## Implementation details

Added the common HTTP package with:

- `new_id(prefix)` using UUIDv7 and lowercase alphabetic prefixes.
- Stable `ErrorCode`, `ErrorBody`, `ErrorEnvelope`, and `ApiError` types.
- Request context middleware that requires `X-Correlation-Id` on `/v1/` requests, creates opaque request IDs, propagates `traceparent`, and emits `X-Request-Id` and `X-Scenario-Epoch` response headers.
- Live and readiness health routes.
- `create_app` with the public health and OpenAPI routes, supplied routers, API error handling, and validation error envelopes.

## TDD evidence

RED command:

```text
uv run pytest tests/unit/common/http/test_app.py -q
```

Relevant RED output:

```text
ModuleNotFoundError: No module named 'enterprise_twins.common'
1 error during collection
```

GREEN command:

```text
uv run pytest tests/unit/common/http/test_app.py -q
```

Relevant GREEN output:

```text
3 passed, 1 warning in 0.32s
```

The warning is the existing Starlette deprecation warning for using `httpx` with `starlette.testclient`.

## Full suite and static checks

Focused checks passed:

```text
uv run ruff check src/enterprise_twins/common tests/unit/common
All checks passed!

uv run ruff format --check src/enterprise_twins/common tests/unit/common
8 files already formatted

uv run mypy
Success: no issues found in 8 source files
```

The full command `uv run pytest -q` ran 5 tests and produced 4 passes and 1 failure. The existing integration test `tests/integration/test_database_isolation.py::test_identity_login_is_limited_to_identity_database` cannot resolve the Compose hostname `postgres` in this environment. The failure occurs before any application code runs.

## Self-review findings

Inspected the complete untracked diff and ran `git diff --check`; no whitespace errors or unrelated edits were found. The implementation is limited to the files named in the task brief.

## Concerns

The full suite requires the Compose database service to be running and resolvable as `postgres`. The focused Task 2 contract tests and all static checks pass.
