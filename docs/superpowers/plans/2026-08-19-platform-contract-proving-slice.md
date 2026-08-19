# Platform-contract proving slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the shared twin platform plus working Identity and CRM twins, then prove authentication, isolated persistence, reset, virtual time, faults, idempotency, events, webhook delivery, and restart survival through public APIs.

**Architecture:** One Python distribution builds a common container image, but every running component has its own process and database credential. Identity and CRM expose public APIs, separate admin processes expose reset operations only on `twin-control`, Event Relay uses an integration-only API process and a listener-free delivery worker, and Control owns scenario epoch, virtual time, and fault activation. PostgreSQL stores each service in a separate database; no twin imports another twin's domain models or repository.

**Tech Stack:** Python 3.14, uv with a committed lockfile, FastAPI, Pydantic 2, SQLAlchemy 2 asyncio, asyncpg, Alembic, PyJWT with Ed25519, HTTPX, Typer, PostgreSQL 18, Docker Compose, pytest, pytest-asyncio, Ruff, and mypy.

**Spec:** `docs/superpowers/specs/2026-08-19-enterprise-digital-twins-design.md`

**Implementation references:** [uv projects](https://docs.astral.sh/uv/concepts/projects/), [FastAPI lifespan](https://fastapi.tiangolo.com/advanced/events/), [FastAPI containers](https://fastapi.tiangolo.com/deployment/docker/), [SQLAlchemy asyncio](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html), [Docker Compose startup order](https://docs.docker.com/compose/how-tos/startup-order/), and [PyJWT usage](https://pyjwt.readthedocs.io/en/stable/usage.html).

## Global Constraints

- Implement only the platform-contract proving slice. The remaining system plans are listed in `docs/superpowers/plans/2026-08-19-enterprise-digital-twins-programme.md`.
- The SUT owns workflow state, ordering, decisions, approvals, retries, explanations, and completion.
- Identity owns principals, roles, scopes, tokens, and verification keys. CRM owns customer profiles, contact methods, external identifiers, and general account notes.
- Public APIs use `/v1`, bearer authentication, `X-Correlation-Id`, the common error envelope, opaque identifiers, RFC 3339 UTC timestamps, and monotonic resource versions.
- Every state-changing public request requires `Idempotency-Key`. Every update of a mutable resource also requires `If-Match`.
- An idempotency namespace is tenant, caller identity, service operation, and key. Same data returns the original result; changed data returns HTTP 409.
- A source write, audit record, idempotency result, and outbox event commit in one local database transaction.
- Business behaviour reads virtual time through Control. Host wall time is limited to process health, client timeouts, and log ingestion metadata.
- Public and control routes run in separate processes. A public process must not mount a `/control` or `/internal/reset` route.
- The Event Relay delivery worker has no HTTP listener on `twin-webhook-egress`.
- PostgreSQL is not published to the host. Each service has a separate database and login, and tests prove cross-database denial.
- State survives application-container restart. Reset acts on running containers and does not recreate or restart them.
- Public black-box tests must not read a twin database or use the Docker socket.
- Synthetic credentials and signing material are local test data only. Logs and errors must not contain client secrets, bearer tokens, restricted values, or webhook signing secrets.
- Use Canadian English in documentation. Do not use em dashes.
- The directory is not a Git repository at planning time. Task 1 creates a local repository and baseline commit; no task configures a remote or pushes.

---

## Programme boundary

This plan creates the contracts consumed by every later twin plan. The public
business proof uses real Identity and CRM behaviour. Test-only receiver and
driver processes observe webhooks and administer the private control plane;
they contain no business rules.

```mermaid
flowchart LR
    Driver[Conformance driver] -->|public API| Identity
    Driver -->|public API| CRM
    Driver -->|private API| Control
    Identity -->|outbox batches| RelayAPI[Event Relay API]
    CRM -->|outbox batches| RelayAPI
    RelayAPI --> DB[(Relay database)]
    DB --> RelayWorker[Listener-free relay worker]
    RelayWorker -->|signed webhook| Receiver[Test receiver]
    Control --> IdentityAdmin[Identity admin]
    Control --> CRMAdmin[CRM admin]
    Control --> RelayAdmin[Relay admin]
    Identity -->|virtual now and fault evaluation| Control
    CRM -->|virtual now and fault evaluation| Control
```

## File map

| Path | Responsibility |
|---|---|
| `.python-version` | Select Python 3.14 for uv |
| `pyproject.toml` | Package metadata, dependency bounds, CLI entry points, and tool configuration |
| `uv.lock` | Exact Python dependency resolution |
| `.gitignore`, `.dockerignore` | Exclude local state and build inputs |
| `Dockerfile` | Build the one immutable Python image used by all application processes |
| `compose.yaml` | Define networks, databases, public apps, admin apps, relay worker, Control, and test profile |
| `docker/postgres/init/001-create-databases.sql` | Create isolated service databases and logins |
| `src/enterprise_twins/common/canonical.py` | Canonical JSON bytes and SHA-256 request or manifest digests |
| `src/enterprise_twins/common/ids.py` | Prefixed opaque UUIDv7 identifiers |
| `src/enterprise_twins/common/settings.py` | Shared environment settings and secret-safe configuration |
| `src/enterprise_twins/common/http/app.py` | FastAPI application factory and exception handlers |
| `src/enterprise_twins/common/http/context.py` | Correlation, request ID, actor, trace, epoch, and response metadata |
| `src/enterprise_twins/common/http/errors.py` | Stable error codes and envelope |
| `src/enterprise_twins/common/http/health.py` | Liveness, readiness, and capability routes |
| `src/enterprise_twins/common/auth/claims.py` | Typed token claims and scope checks |
| `src/enterprise_twins/common/auth/verifier.py` | Cached JWKS retrieval and EdDSA token verification |
| `src/enterprise_twins/common/db/base.py` | SQLAlchemy base, UTC type, version mixin, and scenario-epoch mixin |
| `src/enterprise_twins/common/db/runtime.py` | Async engine, session factory, and transaction lifecycle |
| `src/enterprise_twins/common/db/records.py` | Scenario state, audit, idempotency, and outbox tables owned in each service database |
| `src/enterprise_twins/common/db/idempotency.py` | Durable reserve, replay, mismatch, and completion behaviour |
| `src/enterprise_twins/common/events/contracts.py` | Event, subscription, and delivery request schemas |
| `src/enterprise_twins/common/events/publisher.py` | Transactional outbox creation and source dispatcher |
| `src/enterprise_twins/common/events/relay_client.py` | Integration-only Event Relay client |
| `src/enterprise_twins/common/control/contracts.py` | Clock, fault, reset, and scenario schemas |
| `src/enterprise_twins/common/control/auth.py` | Constant-time private bearer-token dependency |
| `src/enterprise_twins/common/control/client.py` | Private Control client used by twins |
| `src/enterprise_twins/common/control/participant.py` | Epoch-staged reset participant state machine |
| `src/enterprise_twins/migration_metadata.py` | Select the tables owned by one service |
| `src/enterprise_twins/migration_runner.py` | Run one service's Alembic revision chain |
| `src/enterprise_twins/alembic/` | Shared environment and revisions that branch by service ownership |
| `src/enterprise_twins/services/control/` | Control API, CLI, reset coordinator, virtual clock, fault store, and models |
| `src/enterprise_twins/services/relay/` | Integration API, subscription and delivery store, worker, admin app, and models |
| `src/enterprise_twins/services/identity/` | OIDC metadata, JWKS, token, self-view, clients, admin app, and seed loader |
| `src/enterprise_twins/services/crm/` | Customer search, customer read, notes, admin app, and seed loader |
| `scenarios/base/platform-v1/` | Versioned manifest and deterministic Identity, CRM, and Relay seed documents |
| `tests/unit/` | Pure contract, digest, scope, fault, and state-machine tests |
| `tests/contract/` | Per-service ASGI contract tests with PostgreSQL |
| `tests/integration/` | Compose network, reset, relay, and persistence tests |
| `src/enterprise_twins/conformance/receiver.py` | Test-profile webhook receiver and private observation API |
| `src/enterprise_twins/conformance/platform_contracts.py` | Black-box public and control API sequence |
| `scripts/conformance` | Host wrapper that runs phases and performs the required app restart |
| `docs/development.md` | Build, run, reset, test, and diagnosis commands |

### Task 1: Reproducible Python and PostgreSQL workspace

**Files:**
- Create: `.python-version`
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `.dockerignore`
- Create: `Dockerfile`
- Create: `compose.yaml`
- Create: `docker/postgres/init/001-create-databases.sql`
- Create: `src/enterprise_twins/__init__.py`
- Create: `tests/unit/test_package.py`

**Interfaces:**
- Consumes: The approved design and an installed `git`, `uv`, and Docker Compose.
- Produces: importable package `enterprise_twins`; `enterprise_twins.__version__: str`; Compose services `postgres` and test-profile `test-runner`; databases and users named `control`, `relay`, `identity`, `crm`, and `platform_test`.

- [ ] **Step 1: Initialise the local repository and preserve the approved documents**

Run:

```bash
git init -b main
git add enterprise-workflow-exercises.md docs/superpowers/specs docs/superpowers/plans
git commit -m "docs: establish enterprise twin design baseline"
```

Expected: `git status --short` prints no tracked changes.

- [ ] **Step 2: Write the package smoke test**

```python
# tests/unit/test_package.py
from enterprise_twins import __version__


def test_package_has_release_version() -> None:
    assert __version__ == "0.1.0"
```

- [ ] **Step 3: Run the smoke test to verify it fails**

Run: `uv run pytest tests/unit/test_package.py -q`  
Expected: FAIL because `enterprise_twins` is not importable.

- [ ] **Step 4: Add the package and locked tool configuration**

```text
# .python-version
3.14
```

```toml
# pyproject.toml
[build-system]
requires = ["uv_build>=0.12,<0.13"]
build-backend = "uv_build"

[project]
name = "enterprise-twins"
version = "0.1.0"
requires-python = ">=3.14,<3.15"
dependencies = [
  "alembic>=1.16,<2",
  "asyncpg>=0.30,<1",
  "cryptography>=45,<47",
  "fastapi>=0.116,<1",
  "httpx>=0.28,<1",
  "pydantic-settings>=2.10,<3",
  "pyjwt>=2.10,<3",
  "python-multipart>=0.0.20,<1",
  "sqlalchemy[asyncio]>=2.0.43,<3",
  "structlog>=25.4,<27",
  "typer>=0.16,<1",
  "uvicorn[standard]>=0.35,<1",
]

[project.scripts]
twins = "enterprise_twins.services.control.cli:app"

[dependency-groups]
dev = [
  "mypy>=1.17,<2",
  "pytest>=8.4,<10",
  "pytest-asyncio>=1.1,<2",
  "ruff>=0.12,<1",
]

[tool.pytest.ini_options]
addopts = "--strict-config --strict-markers"
asyncio_mode = "auto"
testpaths = ["tests"]

[tool.ruff]
line-length = 100
target-version = "py314"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "ASYNC", "S", "RUF"]

[tool.mypy]
python_version = "3.14"
strict = true
packages = ["enterprise_twins"]
```

```python
# src/enterprise_twins/__init__.py
__version__ = "0.1.0"
```

Run: `uv lock && uv sync --locked --all-groups`  
Expected: `uv.lock` is created and the environment resolves without an error.

- [ ] **Step 5: Add local-state exclusions and the common image**

```gitignore
# .gitignore
.venv/
.pytest_cache/
.mypy_cache/
.ruff_cache/
__pycache__/
*.pyc
.env
artifacts/
```

```text
# .dockerignore
.git
.venv
.pytest_cache
.mypy_cache
.ruff_cache
artifacts
__pycache__
```

```dockerfile
# Dockerfile
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS build
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN uv sync --locked --no-dev

FROM build AS test
RUN uv sync --locked --all-groups
ENTRYPOINT ["uv", "run"]

FROM python:3.14-slim-bookworm
WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
COPY --from=build /app /app
USER 65532:65532
ENTRYPOINT ["python", "-m"]
```

- [ ] **Step 6: Create isolated databases and the first Compose service**

```sql
-- docker/postgres/init/001-create-databases.sql
CREATE USER control_user PASSWORD 'control_local_only';
CREATE USER relay_user PASSWORD 'relay_local_only';
CREATE USER identity_user PASSWORD 'identity_local_only';
CREATE USER crm_user PASSWORD 'crm_local_only';
CREATE USER platform_test_user PASSWORD 'platform_test_local_only';

CREATE DATABASE control OWNER control_user;
CREATE DATABASE relay OWNER relay_user;
CREATE DATABASE identity OWNER identity_user;
CREATE DATABASE crm OWNER crm_user;
CREATE DATABASE platform_test OWNER platform_test_user;

REVOKE CONNECT ON DATABASE control FROM PUBLIC;
REVOKE CONNECT ON DATABASE relay FROM PUBLIC;
REVOKE CONNECT ON DATABASE identity FROM PUBLIC;
REVOKE CONNECT ON DATABASE crm FROM PUBLIC;
REVOKE CONNECT ON DATABASE platform_test FROM PUBLIC;
GRANT CONNECT ON DATABASE control TO control_user;
GRANT CONNECT ON DATABASE relay TO relay_user;
GRANT CONNECT ON DATABASE identity TO identity_user;
GRANT CONNECT ON DATABASE crm TO crm_user;
GRANT CONNECT ON DATABASE platform_test TO platform_test_user;
```

```yaml
# compose.yaml, initial content
name: enterprise-twins
services:
  postgres:
    image: postgres:18-alpine
    environment:
      POSTGRES_USER: twins_admin
      POSTGRES_PASSWORD: local_admin_only
      POSTGRES_DB: postgres
    volumes:
      - twin-postgres:/var/lib/postgresql/data
      - ./docker/postgres/init:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U twins_admin -d postgres"]
      interval: 2s
      timeout: 2s
      retries: 30
    networks: [twin-integration, twin-control]

  test-runner:
    profiles: [test]
    build:
      context: .
      target: test
    environment:
      TEST_DATABASE_URL: postgresql+asyncpg://platform_test_user:platform_test_local_only@postgres/platform_test
    depends_on:
      postgres:
        condition: service_healthy
    networks: [twin-integration, twin-control]
    command: ["python", "-c", "import time; time.sleep(10**9)"]

volumes:
  twin-postgres:

networks:
  twin-public: {}
  twin-integration:
    internal: true
  twin-webhook-egress:
    internal: true
  twin-control:
    internal: true
```

- [ ] **Step 7: Verify imports and database login isolation**

Run:

```bash
uv run pytest tests/unit/test_package.py -q
docker compose up -d --wait postgres
docker compose exec -T postgres psql postgresql://identity_user:identity_local_only@localhost/identity -c 'select current_user, current_database()'
docker compose exec -T postgres sh -c '! psql postgresql://identity_user:identity_local_only@localhost/crm -c "select 1"'
```

Expected: pytest passes, the first query returns `identity_user | identity`, and the CRM connection using the Identity login is denied.

- [ ] **Step 8: Run static checks and commit**

Run:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy
git add .python-version pyproject.toml uv.lock .gitignore .dockerignore Dockerfile compose.yaml docker src tests
git commit -m "build: establish twin runtime and isolated databases"
```

Expected: all checks pass and the commit contains no local database volume or credential file.

### Task 2: Common HTTP contract

**Files:**
- Create: `src/enterprise_twins/common/ids.py`
- Create: `src/enterprise_twins/common/http/errors.py`
- Create: `src/enterprise_twins/common/http/context.py`
- Create: `src/enterprise_twins/common/http/health.py`
- Create: `src/enterprise_twins/common/http/app.py`
- Create: `tests/unit/common/http/test_app.py`

**Interfaces:**
- Consumes: `enterprise_twins` package from Task 1.
- Produces: `new_id(prefix: str) -> str`; `ApiError`; `ErrorCode`; `RequestContext`; `RuntimeStatus` protocol; `create_app(name: str, capabilities: Sequence[str], status: RuntimeStatus, routers: Sequence[APIRouter] = (), lifespan: Callable | None = None) -> FastAPI`.

- [ ] **Step 1: Write public-contract tests**

```python
# tests/unit/common/http/test_app.py
from fastapi import APIRouter
from fastapi.testclient import TestClient

from enterprise_twins.common.http.app import create_app
from enterprise_twins.common.http.errors import ApiError, ErrorCode


class ReadyStatus:
    async def current_epoch(self) -> str:
        return "epoch_test"

    async def readiness(self) -> tuple[bool, dict[str, str]]:
        return True, {"database": "ready"}


router = APIRouter()


@router.get("/v1/failure")
async def failure() -> None:
    raise ApiError(ErrorCode.CONFLICT, "version changed", status_code=409)


client = TestClient(create_app("probe", ("probe:read",), ReadyStatus(), (router,)))


def test_health_and_openapi_are_public() -> None:
    assert client.get("/health/live").json() == {"status": "live"}
    assert client.get("/health/ready").status_code == 200
    assert client.get("/openapi.json").json()["openapi"].startswith("3.1")


def test_business_request_requires_correlation_id() -> None:
    response = client.get("/v1/failure")
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_request"


def test_error_envelope_and_response_metadata() -> None:
    response = client.get("/v1/failure", headers={"X-Correlation-Id": "case-123"})
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "conflict"
    assert response.json()["error"]["requestId"].startswith("req_")
    assert response.headers["X-Scenario-Epoch"] == "epoch_test"
    assert response.headers["X-Request-Id"].startswith("req_")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/common/http/test_app.py -q`  
Expected: FAIL because `enterprise_twins.common.http` does not exist.

- [ ] **Step 3: Implement IDs and the stable error envelope**

```python
# src/enterprise_twins/common/ids.py
from uuid import uuid7


def new_id(prefix: str) -> str:
    if not prefix.isalpha() or not prefix.islower():
        raise ValueError("identifier prefix must contain lowercase letters")
    return f"{prefix}_{uuid7().hex}"
```

```python
# src/enterprise_twins/common/http/errors.py
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class ErrorCode(StrEnum):
    INVALID_REQUEST = "invalid_request"
    UNAUTHENTICATED = "unauthenticated"
    FORBIDDEN = "forbidden"
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"
    PRECONDITION_FAILED = "precondition_failed"
    RATE_LIMITED = "rate_limited"
    TEMPORARILY_UNAVAILABLE = "temporarily_unavailable"
    INTERNAL_ERROR = "internal_error"


class ErrorBody(BaseModel):
    code: ErrorCode
    message: str
    retryable: bool = False
    requestId: str
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorEnvelope(BaseModel):
    error: ErrorBody


class ApiError(Exception):
    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        status_code: int,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.retryable = retryable
        self.details = details or {}
```

- [ ] **Step 4: Implement request context and response metadata**

```python
# src/enterprise_twins/common/http/context.py
from collections.abc import Awaitable, Callable, Sequence
from contextvars import ContextVar
from dataclasses import dataclass

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from enterprise_twins.common.http.errors import ErrorBody, ErrorCode, ErrorEnvelope
from enterprise_twins.common.ids import new_id


@dataclass(frozen=True, slots=True)
class RequestContext:
    request_id: str
    correlation_id: str
    traceparent: str | None


current_request: ContextVar[RequestContext | None] = ContextVar("current_request", default=None)


class RequestContextMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: object, epoch: Callable[[], Awaitable[str]]) -> None:
        super().__init__(app)  # type: ignore[arg-type]
        self.epoch = epoch

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request_id = new_id("req")
        correlation_id = request.headers.get("X-Correlation-Id")
        if request.url.path.startswith("/v1/") and not correlation_id:
            body = ErrorEnvelope(
                error=ErrorBody(
                    code=ErrorCode.INVALID_REQUEST,
                    message="X-Correlation-Id is required",
                    requestId=request_id,
                )
            )
            response = JSONResponse(body.model_dump(mode="json"), status_code=400)
            response.headers["X-Request-Id"] = request_id
            response.headers["X-Scenario-Epoch"] = await self.epoch()
            return response
        context = RequestContext(request_id, correlation_id or request_id, request.headers.get("traceparent"))
        token = current_request.set(context)
        try:
            response = await call_next(request)
        finally:
            current_request.reset(token)
        response.headers["X-Request-Id"] = request_id
        response.headers["X-Scenario-Epoch"] = await self.epoch()
        if context.traceparent:
            response.headers["traceparent"] = context.traceparent
        return response
```

- [ ] **Step 5: Implement health routes and the application factory**

```python
# src/enterprise_twins/common/http/health.py
from typing import Protocol

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from starlette.responses import Response


class RuntimeStatus(Protocol):
    async def current_epoch(self) -> str:
        raise NotImplementedError

    async def readiness(self) -> tuple[bool, dict[str, str]]:
        raise NotImplementedError


def health_router(status: RuntimeStatus) -> APIRouter:
    router = APIRouter()

    @router.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "live"}

    @router.get("/health/ready", response_model=None)
    async def ready() -> Response:
        is_ready, checks = await status.readiness()
        body = {"status": "ready" if is_ready else "not_ready", "checks": checks}
        return JSONResponse(body, status_code=200 if is_ready else 503)

    return router
```

```python
# src/enterprise_twins/common/http/app.py
from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager

from fastapi import APIRouter, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from enterprise_twins.common.http.context import RequestContextMiddleware, current_request
from enterprise_twins.common.http.errors import ApiError, ErrorBody, ErrorEnvelope
from enterprise_twins.common.http.health import RuntimeStatus, health_router
from enterprise_twins.common.ids import new_id


def create_app(
    name: str,
    capabilities: Sequence[str],
    status: RuntimeStatus,
    routers: Sequence[APIRouter] = (),
    lifespan: Callable[[FastAPI], AbstractAsyncContextManager[None]] | None = None,
) -> FastAPI:
    app = FastAPI(
        title=name,
        version="0.1.0",
        openapi_version="3.1.0",
        lifespan=lifespan,
    )
    app.state.capabilities = capabilities
    app.add_middleware(RequestContextMiddleware, epoch=status.current_epoch)
    app.include_router(health_router(status))
    for router in routers:
        app.include_router(router)

    @app.exception_handler(ApiError)
    async def api_error(_request: Request, error: ApiError) -> JSONResponse:
        context = current_request.get()
        request_id = context.request_id if context else new_id("req")
        body = ErrorEnvelope(
            error=ErrorBody(
                code=error.code,
                message=error.message,
                retryable=error.retryable,
                requestId=request_id,
                details=error.details,
            )
        )
        response = JSONResponse(body.model_dump(mode="json"), status_code=error.status_code)
        response.headers["X-Request-Id"] = request_id
        response.headers["X-Scenario-Epoch"] = await status.current_epoch()
        return response

    @app.exception_handler(RequestValidationError)
    async def invalid_request(_request: Request, error: RequestValidationError) -> JSONResponse:
        context = current_request.get()
        request_id = context.request_id if context else new_id("req")
        body = ErrorEnvelope(
            error=ErrorBody(
                code="invalid_request",
                message="request validation failed",
                requestId=request_id,
                details={"errors": error.errors(include_url=False, include_context=False, include_input=False)},
            )
        )
        response = JSONResponse(body.model_dump(mode="json"), status_code=422)
        response.headers["X-Request-Id"] = request_id
        response.headers["X-Scenario-Epoch"] = await status.current_epoch()
        return response

    return app
```

- [ ] **Step 6: Run focused tests**

Run: `uv run pytest tests/unit/common/http/test_app.py -q`  
Expected: all three tests pass.

- [ ] **Step 7: Run contract checks and commit**

Run:

```bash
uv run ruff check src/enterprise_twins/common tests/unit/common
uv run ruff format --check src/enterprise_twins/common tests/unit/common
uv run mypy
git add src/enterprise_twins/common tests/unit/common
git commit -m "feat: define common HTTP contract"
```

Expected: all checks pass.

### Task 3: Transactional audit, idempotency, and outbox records

**Files:**
- Create: `src/enterprise_twins/common/canonical.py`
- Create: `src/enterprise_twins/common/db/base.py`
- Create: `src/enterprise_twins/common/db/runtime.py`
- Create: `src/enterprise_twins/common/db/records.py`
- Create: `src/enterprise_twins/common/db/idempotency.py`
- Create: `src/enterprise_twins/common/events/contracts.py`
- Create: `src/enterprise_twins/common/events/publisher.py`
- Create: `tests/conftest.py`
- Create: `tests/contract/common/test_transactional_records.py`

**Interfaces:**
- Consumes: `ApiError`, `ErrorCode`, and `new_id` from Task 2; PostgreSQL `platform_test` database from Task 1.
- Produces: `canonical_json(value: object) -> bytes`; `sha256_hex(value: object) -> str`; `Base`; `make_engine(url: str) -> AsyncEngine`; `make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]`; `ScenarioState`, `AuditRecord`, `IdempotencyRecord`, `OutboxRecord`; `IdempotencyNamespace`; `StoredResponse`; `run_idempotent`; `EventEnvelope`; `record_audit`; `record_event`.

- [ ] **Step 1: Write the transactional-record contract tests**

```python
# tests/contract/common/test_transactional_records.py
from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.db.idempotency import (
    IdempotencyNamespace,
    StoredResponse,
    run_idempotent,
)
from enterprise_twins.common.db.records import AuditRecord, IdempotencyRecord, OutboxRecord
from enterprise_twins.common.events.publisher import record_audit, record_event
from enterprise_twins.common.http.errors import ApiError


async def count(factory: async_sessionmaker[AsyncSession], model: type[object]) -> int:
    async with factory() as session:
        return int((await session.scalar(select(func.count()).select_from(model))) or 0)


@pytest.mark.asyncio
async def test_same_idempotency_input_replays_original_result(
    db: async_sessionmaker[AsyncSession],
) -> None:
    calls = 0
    namespace = IdempotencyNamespace("tenant_test", "actor_test", "crm.note.create", "idem-1")

    async def work() -> StoredResponse:
        nonlocal calls
        calls += 1
        return StoredResponse(201, {"noteId": "note_1"}, {"ETag": '"1"'})

    async with db.begin() as session:
        first, first_replay = await run_idempotent(session, "epoch_1", namespace, {"body": "VIP"}, work)
    async with db.begin() as session:
        second, second_replay = await run_idempotent(session, "epoch_1", namespace, {"body": "VIP"}, work)

    assert first == second
    assert first_replay is False
    assert second_replay is True
    assert calls == 1


@pytest.mark.asyncio
async def test_changed_input_under_same_key_is_conflict(
    db: async_sessionmaker[AsyncSession],
) -> None:
    namespace = IdempotencyNamespace("tenant_test", "actor_test", "crm.note.create", "idem-2")

    async def work() -> StoredResponse:
        return StoredResponse(201, {"noteId": "note_2"}, {})

    async with db.begin() as session:
        await run_idempotent(session, "epoch_1", namespace, {"body": "first"}, work)
    with pytest.raises(ApiError) as raised:
        async with db.begin() as session:
            await run_idempotent(session, "epoch_1", namespace, {"body": "changed"}, work)
    assert raised.value.status_code == 409
    assert raised.value.details == {"operation": "crm.note.create"}


@pytest.mark.asyncio
async def test_domain_audit_idempotency_and_event_roll_back_together(
    db: async_sessionmaker[AsyncSession],
) -> None:
    namespace = IdempotencyNamespace("tenant_test", "actor_test", "probe.create", "idem-3")

    async def work(session: AsyncSession) -> StoredResponse:
        record_audit(
            session,
            epoch="epoch_1",
            action="probe.created",
            resource_type="probe",
            resource_id="probe_1",
            actor_id="actor_test",
            correlation_id="case-1",
            occurred_at=datetime(2026, 8, 19, 10, tzinfo=UTC),
            details={},
        )
        record_event(
            session,
            epoch="epoch_1",
            event_type="probe.created",
            source="probe",
            subject="probe/probe_1",
            resource_version=1,
            correlation_id="case-1",
            causation_id="req-1",
            occurred_at=datetime(2026, 8, 19, 10, tzinfo=UTC),
            data={"probeId": "probe_1"},
        )
        return StoredResponse(201, {"probeId": "probe_1"}, {})

    with pytest.raises(RuntimeError, match="force rollback"):
        async with db.begin() as session:
            await run_idempotent(
                session,
                "epoch_1",
                namespace,
                {"name": "probe"},
                lambda: work(session),
            )
            raise RuntimeError("force rollback")

    assert await count(db, AuditRecord) == 0
    assert await count(db, OutboxRecord) == 0
    assert await count(db, IdempotencyRecord) == 0
```

- [ ] **Step 2: Add the PostgreSQL fixture**

```python
# tests/conftest.py
import os
from pathlib import Path
from collections.abc import AsyncIterator

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.db.base import Base
from enterprise_twins.common.db.runtime import make_engine, make_session_factory


@pytest_asyncio.fixture
async def db() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    url = os.environ["TEST_DATABASE_URL"]
    engine = make_engine(url)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    factory = make_session_factory(engine)
    yield factory
    await engine.dispose()
```

- [ ] **Step 3: Run the contract tests to verify they fail**

Run:

```bash
docker compose build test-runner
docker compose run --rm test-runner pytest tests/contract/common/test_transactional_records.py -q
```

Expected: FAIL because the common database modules do not exist.

- [ ] **Step 4: Implement canonical hashes and database runtime types**

```python
# src/enterprise_twins/common/canonical.py
import hashlib
import json
from datetime import date, datetime
from enum import Enum


def _json_default(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"cannot encode {type(value).__name__} as canonical JSON")


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_default,
    ).encode()


def sha256_hex(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()
```

```python
# src/enterprise_twins/common/db/base.py
from datetime import datetime

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ScenarioOwned:
    scenario_epoch: Mapped[str] = mapped_column(String(64), index=True)


class Versioned:
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


Timestamp = DateTime(timezone=True)
```

```python
# src/enterprise_twins/common/db/runtime.py
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def make_engine(url: str) -> AsyncEngine:
    return create_async_engine(url, pool_pre_ping=True)


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def transaction(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    async with factory.begin() as session:
        yield session
```

- [ ] **Step 5: Implement the service-owned platform tables**

```python
# src/enterprise_twins/common/db/records.py
from datetime import datetime
from typing import Any, Protocol

from sqlalchemy import BigInteger, Boolean, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from enterprise_twins.common.db.base import Base, ScenarioOwned, Timestamp


class ScenarioState(Base):
    __tablename__ = "scenario_state"
    singleton_id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    mode: Mapped[str] = mapped_column(String(24), nullable=False, default="uninitialised")
    active_epoch: Mapped[str] = mapped_column(String(64), nullable=False, default="none")
    pending_epoch: Mapped[str | None] = mapped_column(String(64))
    scenario_id: Mapped[str | None] = mapped_column(String(80))
    scenario_version: Mapped[int | None] = mapped_column(Integer)
    random_seed: Mapped[int | None] = mapped_column(BigInteger)
    manifest_checksum: Mapped[str | None] = mapped_column(String(64))


class AuditRecord(ScenarioOwned, Base):
    __tablename__ = "audit_records"
    audit_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    action: Mapped[str] = mapped_column(String(120), index=True)
    resource_type: Mapped[str] = mapped_column(String(80))
    resource_id: Mapped[str] = mapped_column(String(128), index=True)
    actor_id: Mapped[str] = mapped_column(String(128))
    correlation_id: Mapped[str] = mapped_column(String(128), index=True)
    occurred_at: Mapped[datetime] = mapped_column(Timestamp, nullable=False)
    details: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class IdempotencyRecord(ScenarioOwned, Base):
    __tablename__ = "idempotency_records"
    __table_args__ = (
        UniqueConstraint("tenant_id", "actor_id", "operation", "key", name="uq_idempotency_namespace"),
    )
    record_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(80))
    actor_id: Mapped[str] = mapped_column(String(128))
    operation: Mapped[str] = mapped_column(String(120))
    key: Mapped[str] = mapped_column(String(200))
    request_hash: Mapped[str] = mapped_column(String(64))
    state: Mapped[str] = mapped_column(String(24), nullable=False)
    response_status: Mapped[int | None] = mapped_column(Integer)
    response_body: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    response_headers: Mapped[dict[str, str] | None] = mapped_column(JSONB)


class OutboxRecord(ScenarioOwned, Base):
    __tablename__ = "outbox_records"
    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(160), index=True)
    envelope: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    publish_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    published_at: Mapped[datetime | None] = mapped_column(Timestamp)


Index("ix_outbox_pending", OutboxRecord.published, OutboxRecord.event_id)
```

- [ ] **Step 6: Implement the event envelope and transactional record writers**

```python
# src/enterprise_twins/common/events/contracts.py
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class EventEnvelope(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event_id: str = Field(alias="eventId")
    event_type: str = Field(alias="eventType")
    schema_version: str = Field(default="1.0", alias="schemaVersion")
    source: str
    subject: str
    resource_version: int = Field(alias="resourceVersion")
    correlation_id: str = Field(alias="correlationId")
    causation_id: str = Field(alias="causationId")
    occurred_at: datetime = Field(alias="occurredAt")
    recorded_at: datetime = Field(alias="recordedAt")
    data: dict[str, Any]
```

```python
# src/enterprise_twins/common/events/publisher.py
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from enterprise_twins.common.db.records import AuditRecord, OutboxRecord, ScenarioState
from enterprise_twins.common.events.contracts import EventEnvelope
from enterprise_twins.common.ids import new_id


def record_audit(
    session: AsyncSession,
    *,
    epoch: str,
    action: str,
    resource_type: str,
    resource_id: str,
    actor_id: str,
    correlation_id: str,
    occurred_at: datetime,
    details: dict[str, Any],
) -> AuditRecord:
    record = AuditRecord(
        audit_id=new_id("aud"),
        scenario_epoch=epoch,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        actor_id=actor_id,
        correlation_id=correlation_id,
        occurred_at=occurred_at,
        details=details,
    )
    session.add(record)
    return record


def record_event(
    session: AsyncSession,
    *,
    epoch: str,
    event_type: str,
    source: str,
    subject: str,
    resource_version: int,
    correlation_id: str,
    causation_id: str,
    occurred_at: datetime,
    data: dict[str, Any],
) -> EventEnvelope:
    envelope = EventEnvelope(
        eventId=new_id("evt"),
        eventType=event_type,
        source=source,
        subject=subject,
        resourceVersion=resource_version,
        correlationId=correlation_id,
        causationId=causation_id,
        occurredAt=occurred_at,
        recordedAt=datetime.now(UTC),
        data=data,
    )
    session.add(
        OutboxRecord(
            event_id=envelope.event_id,
            scenario_epoch=epoch,
            event_type=event_type,
            envelope=envelope.model_dump(mode="json", by_alias=True),
            published=False,
            publish_attempts=0,
        )
    )
    return envelope
```

- [ ] **Step 7: Implement durable idempotency**

```python
# src/enterprise_twins/common/db/idempotency.py
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.db.records import IdempotencyRecord
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.common.ids import new_id


@dataclass(frozen=True, slots=True)
class IdempotencyNamespace:
    tenant_id: str
    actor_id: str
    operation: str
    key: str


@dataclass(frozen=True, slots=True)
class StoredResponse:
    status_code: int
    body: dict[str, Any]
    headers: dict[str, str]


async def run_idempotent(
    session: AsyncSession,
    epoch: str,
    namespace: IdempotencyNamespace,
    request_payload: object,
    work: Callable[[], Awaitable[StoredResponse]],
) -> tuple[StoredResponse, bool]:
    request_hash = sha256_hex(request_payload)
    record_id = new_id("idem")
    statement = (
        insert(IdempotencyRecord)
        .values(
            record_id=record_id,
            scenario_epoch=epoch,
            tenant_id=namespace.tenant_id,
            actor_id=namespace.actor_id,
            operation=namespace.operation,
            key=namespace.key,
            request_hash=request_hash,
            state="reserved",
        )
        .on_conflict_do_nothing(constraint="uq_idempotency_namespace")
        .returning(IdempotencyRecord.record_id)
    )
    inserted = await session.scalar(statement)
    if inserted:
        record = await session.get(IdempotencyRecord, inserted)
        if record is None:
            raise RuntimeError("inserted idempotency record is missing")
        result = await work()
        record.state = "completed"
        record.response_status = result.status_code
        record.response_body = result.body
        record.response_headers = result.headers
        return result, False

    existing = await session.scalar(
        select(IdempotencyRecord)
        .where(
            IdempotencyRecord.tenant_id == namespace.tenant_id,
            IdempotencyRecord.actor_id == namespace.actor_id,
            IdempotencyRecord.operation == namespace.operation,
            IdempotencyRecord.key == namespace.key,
        )
        .with_for_update()
    )
    if existing is None:
        raise RuntimeError("conflicting idempotency record is missing")
    if existing.request_hash != request_hash:
        raise ApiError(
            ErrorCode.CONFLICT,
            "Idempotency-Key was already used with different request data",
            status_code=409,
            details={"operation": namespace.operation},
        )
    if existing.state != "completed":
        raise ApiError(
            ErrorCode.TEMPORARILY_UNAVAILABLE,
            "The original request is still in progress",
            status_code=503,
            retryable=True,
        )
    if existing.response_status is None or existing.response_body is None:
        raise RuntimeError("completed idempotency record has no response")
    return (
        StoredResponse(
            existing.response_status,
            existing.response_body,
            existing.response_headers or {},
        ),
        True,
    )
```

- [ ] **Step 8: Run the focused tests twice**

Run:

```bash
docker compose build test-runner
docker compose run --rm test-runner pytest tests/contract/common/test_transactional_records.py -q
docker compose run --rm test-runner pytest tests/contract/common/test_transactional_records.py -q
```

Expected: three tests pass in both runs, which also proves test setup can recreate its schema.

- [ ] **Step 9: Run quality checks and commit**

Run:

```bash
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy
git add src/enterprise_twins/common tests
git commit -m "feat: add transactional platform records"
```

Expected: all checks pass.

### Task 4: Control-owned virtual clock

**Files:**
- Create: `src/enterprise_twins/common/control/contracts.py`
- Create: `src/enterprise_twins/common/control/auth.py`
- Create: `src/enterprise_twins/common/control/client.py`
- Create: `src/enterprise_twins/services/control/models.py`
- Create: `src/enterprise_twins/services/control/repository.py`
- Create: `src/enterprise_twins/services/control/time.py`
- Create: `src/enterprise_twins/services/control/settings.py`
- Create: `src/enterprise_twins/services/control/api.py`
- Create: `src/enterprise_twins/services/control/app.py`
- Create: `tests/contract/control/test_clock.py`

**Interfaces:**
- Consumes: database runtime and `ScenarioState` from Task 3; common HTTP factory from Task 2.
- Produces: `parse_duration(value: str) -> timedelta`; `ControlRepository.now()`, `set_time()`, and `advance_time()`; private endpoints `GET /control/v1/time`, `PUT /control/v1/time`, and `POST /control/v1/time/advance`; `ControlClient.now() -> datetime`.

- [ ] **Step 1: Write virtual-clock tests**

```python
# tests/contract/control/test_clock.py
from datetime import UTC, datetime, timedelta

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.contracts import FaultDecision
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.services.control.app import create_control_app
from enterprise_twins.services.control.models import VirtualClock
from enterprise_twins.services.control.settings import ControlSettings
from enterprise_twins.services.control.time import parse_duration


@pytest.mark.asyncio
async def test_virtual_clock_set_and_advance(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(
            ScenarioState(
                singleton_id=1,
                mode="active",
                active_epoch="epoch_1",
                scenario_id="platform-contracts",
                scenario_version=1,
                random_seed=7,
                manifest_checksum="a" * 64,
            )
        )
        session.add(VirtualClock(singleton_id=1, now=datetime(2026, 8, 19, 10, tzinfo=UTC)))

    app = create_control_app(
        db,
        ControlSettings(
            database_url="postgresql+asyncpg://unused",
            controller_token="controller-test-token",
            twin_token="twin-test-token",
        ),
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://control") as client:
        denied = await client.get("/control/v1/time")
        current = await client.get(
            "/control/v1/time", headers={"Authorization": "Bearer twin-test-token"}
        )
        advanced = await client.post(
            "/control/v1/time/advance",
            headers={"Authorization": "Bearer controller-test-token"},
            json={"duration": "PT5M"},
        )

    assert denied.status_code == 401
    assert current.json() == {"now": "2026-08-19T10:00:00Z", "scenarioEpoch": "epoch_1"}
    assert advanced.json() == {"now": "2026-08-19T10:05:00Z", "scenarioEpoch": "epoch_1"}
    assert parse_duration("P1DT2H3M4S") == timedelta(days=1, hours=2, minutes=3, seconds=4)


def test_duration_rejects_calendar_units_and_negative_values() -> None:
    with pytest.raises(ValueError):
        parse_duration("P1M")
    with pytest.raises(ValueError):
        parse_duration("-PT1S")
```

- [ ] **Step 2: Run the clock tests to verify they fail**

Run: `docker compose run --rm test-runner pytest tests/contract/control/test_clock.py -q`  
Expected: FAIL because the Control modules do not exist.

- [ ] **Step 3: Define the clock contracts and strict duration parser**

```python
# src/enterprise_twins/common/control/contracts.py
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class ClockValue(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    now: datetime
    scenario_epoch: str = Field(alias="scenarioEpoch")


class SetClockRequest(BaseModel):
    now: datetime


class AdvanceClockRequest(BaseModel):
    duration: str
```

```python
# src/enterprise_twins/services/control/time.py
import re
from datetime import timedelta


_DURATION = re.compile(
    r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?$"
)


def parse_duration(value: str) -> timedelta:
    match = _DURATION.fullmatch(value)
    if match is None or not any(match.groupdict().values()):
        raise ValueError("duration must be a positive ISO 8601 day-time duration")
    duration = timedelta(**{name: int(raw or 0) for name, raw in match.groupdict().items()})
    if duration <= timedelta(0):
        raise ValueError("duration must be greater than zero")
    return duration
```

- [ ] **Step 4: Add the Control clock model and repository**

```python
# src/enterprise_twins/services/control/models.py
from datetime import datetime

from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column

from enterprise_twins.common.db.base import Base, Timestamp


class VirtualClock(Base):
    __tablename__ = "virtual_clock"
    singleton_id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    now: Mapped[datetime] = mapped_column(Timestamp, nullable=False)
```

```python
# src/enterprise_twins/services/control/repository.py
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.services.control.models import VirtualClock


class ControlRepository:
    def __init__(self, factory: async_sessionmaker[AsyncSession]) -> None:
        self.factory = factory

    async def state(self) -> ScenarioState:
        async with self.factory() as session:
            state = await session.get(ScenarioState, 1)
            if state is None:
                raise RuntimeError("control scenario state is not initialised")
            return state

    async def now(self) -> datetime:
        async with self.factory() as session:
            clock = await session.get(VirtualClock, 1)
            if clock is None:
                raise RuntimeError("virtual clock is not initialised")
            return clock.now

    async def set_time(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("virtual time must include a UTC offset")
        async with self.factory.begin() as session:
            clock = await session.scalar(select(VirtualClock).where(VirtualClock.singleton_id == 1).with_for_update())
            if clock is None:
                raise RuntimeError("virtual clock is not initialised")
            clock.now = value
            return clock.now

    async def advance_time(self, amount: timedelta) -> datetime:
        async with self.factory.begin() as session:
            clock = await session.scalar(select(VirtualClock).where(VirtualClock.singleton_id == 1).with_for_update())
            if clock is None:
                raise RuntimeError("virtual clock is not initialised")
            clock.now += amount
            return clock.now
```

- [ ] **Step 5: Add settings, bearer checks, and the Control API**

```python
# src/enterprise_twins/services/control/settings.py
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ControlSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWINS_CONTROL_", extra="ignore")
    database_url: str
    controller_token: str
    twin_token: str
    participant_token: str = "participant-local-token"
    participants: dict[str, str] = Field(default_factory=dict)
    scenario_root: Path = Path("scenarios/base")
    bootstrap_scenario: str = "platform-contracts"
    bootstrap_version: int = 1
```

```python
# src/enterprise_twins/common/control/auth.py
import hmac
from collections.abc import Callable

from fastapi import Header

from enterprise_twins.common.http.errors import ApiError, ErrorCode


def require_token(expected: str) -> Callable[[str | None], None]:
    def check(authorization: str | None = Header(default=None)) -> None:
        supplied = authorization.removeprefix("Bearer ") if authorization else ""
        if not hmac.compare_digest(supplied, expected):
            raise ApiError(ErrorCode.UNAUTHENTICATED, "invalid private credential", status_code=401)

    return check
```

```python
# src/enterprise_twins/services/control/api.py
from typing import Annotated

from fastapi import APIRouter, Depends

from enterprise_twins.common.control.contracts import AdvanceClockRequest, ClockValue, SetClockRequest
from enterprise_twins.common.control.auth import require_token
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.control.repository import ControlRepository
from enterprise_twins.services.control.settings import ControlSettings
from enterprise_twins.services.control.time import parse_duration
def control_router(repository: ControlRepository, settings: ControlSettings) -> APIRouter:
    router = APIRouter(prefix="/control/v1")
    twin_auth = require_token(settings.twin_token)
    controller_auth = require_token(settings.controller_token)
    TwinAuth = Annotated[None, Depends(twin_auth)]
    ControllerAuth = Annotated[None, Depends(controller_auth)]

    @router.get("/time")
    async def get_time(_auth: TwinAuth) -> ClockValue:
        state = await repository.state()
        return ClockValue(now=await repository.now(), scenarioEpoch=state.active_epoch)

    @router.put("/time")
    async def set_time(request: SetClockRequest, _auth: ControllerAuth) -> ClockValue:
        state = await repository.state()
        return ClockValue(now=await repository.set_time(request.now), scenarioEpoch=state.active_epoch)

    @router.post("/time/advance")
    async def advance_time(request: AdvanceClockRequest, _auth: ControllerAuth) -> ClockValue:
        try:
            amount = parse_duration(request.duration)
        except ValueError as error:
            raise ApiError(ErrorCode.INVALID_REQUEST, str(error), status_code=422) from error
        state = await repository.state()
        return ClockValue(now=await repository.advance_time(amount), scenarioEpoch=state.active_epoch)

    return router
```

- [ ] **Step 6: Create the Control app and the reusable clock client**

```python
# src/enterprise_twins/services/control/app.py
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.http.app import create_app
from enterprise_twins.services.control.api import control_router
from enterprise_twins.services.control.repository import ControlRepository
from enterprise_twins.services.control.settings import ControlSettings


class ControlStatus:
    def __init__(self, repository: ControlRepository) -> None:
        self.repository = repository

    async def current_epoch(self) -> str:
        return (await self.repository.state()).active_epoch

    async def readiness(self) -> tuple[bool, dict[str, str]]:
        try:
            state = await self.repository.state()
            await self.repository.now()
        except RuntimeError:
            return False, {"database": "not_ready", "clock": "not_ready"}
        ready = state.mode == "active"
        return ready, {"database": "ready", "clock": "ready", "scenario": state.mode}


def create_control_app(
    factory: async_sessionmaker[AsyncSession], settings: ControlSettings
):
    repository = ControlRepository(factory)
    return create_app(
        "Twin Control",
        ("scenario:reset", "time:write", "faults:write", "diagnostics:read"),
        ControlStatus(repository),
        (control_router(repository, settings),),
    )
```

```python
# src/enterprise_twins/common/control/client.py
from datetime import datetime

import httpx

from enterprise_twins.common.control.contracts import ClockValue


class ControlClient:
    def __init__(self, base_url: str, token: str, client: httpx.AsyncClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.client = client

    async def snapshot(self) -> ClockValue:
        response = await self.client.get(
            f"{self.base_url}/control/v1/time",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        response.raise_for_status()
        return ClockValue.model_validate(response.json())

    async def now(self) -> datetime:
        return (await self.snapshot()).now

    async def current_epoch(self) -> str:
        return (await self.snapshot()).scenario_epoch
```

- [ ] **Step 7: Run the clock tests and quality checks**

Run:

```bash
docker compose build test-runner
docker compose run --rm test-runner pytest tests/contract/control/test_clock.py -q
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy
```

Expected: both clock tests pass and static checks report no errors.

- [ ] **Step 8: Commit**

```bash
git add src/enterprise_twins/common/control src/enterprise_twins/services/control tests/contract/control
git commit -m "feat: add control-owned virtual clock"
```

### Task 5: Deterministic fault rules and activation diagnostics

**Files:**
- Modify: `src/enterprise_twins/common/control/contracts.py`
- Modify: `src/enterprise_twins/common/control/client.py`
- Modify: `src/enterprise_twins/services/control/models.py`
- Create: `src/enterprise_twins/services/control/faults.py`
- Modify: `src/enterprise_twins/services/control/app.py`
- Create: `tests/contract/control/test_faults.py`

**Interfaces:**
- Consumes: Control authentication, database, scenario epoch, and virtual clock from Task 4.
- Produces: `FaultPhase`, `FaultEffect`, `FaultRuleCreate`, `FaultProbe`, `FaultDecision`; `FaultRepository.create()`, `evaluate()`, `list_activations()`, and `clear()`; private fault endpoints; `ControlClient.evaluate_fault(probe: FaultProbe) -> FaultDecision`.

- [ ] **Step 1: Write occurrence, matching, and diagnostics tests**

```python
# tests/contract/control/test_faults.py
from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.contracts import (
    FaultEffect,
    FaultPhase,
    FaultProbe,
    FaultRuleCreate,
)
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.services.control.faults import FaultRepository
from enterprise_twins.services.control.models import FaultActivation, VirtualClock


@pytest.mark.asyncio
async def test_rule_matches_then_fires_at_configured_occurrence_once(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))
        session.add(VirtualClock(singleton_id=1, now=datetime(2026, 8, 19, 10, tzinfo=UTC)))
    repository = FaultRepository(db)
    await repository.create(
        FaultRuleCreate(
            ruleId="crm-note-after-commit",
            targetService="crm",
            operation="crm.note.create",
            phase=FaultPhase.AFTER_COMMIT,
            effect=FaultEffect.TIMEOUT,
            actorId="support-agent",
            occurrence=2,
            activationCount=1,
            delayMs=250,
        )
    )
    wrong_actor = await repository.evaluate(
        FaultProbe(
            targetService="crm",
            operation="crm.note.create",
            phase=FaultPhase.AFTER_COMMIT,
            actorId="auditor",
            correlationId="case-1",
        )
    )
    first = await repository.evaluate(
        FaultProbe(
            targetService="crm",
            operation="crm.note.create",
            phase=FaultPhase.AFTER_COMMIT,
            actorId="support-agent",
            correlationId="case-1",
        )
    )
    second = await repository.evaluate(
        FaultProbe(
            targetService="crm",
            operation="crm.note.create",
            phase=FaultPhase.AFTER_COMMIT,
            actorId="support-agent",
            correlationId="case-1",
        )
    )
    exhausted = await repository.evaluate(
        FaultProbe(
            targetService="crm",
            operation="crm.note.create",
            phase=FaultPhase.AFTER_COMMIT,
            actorId="support-agent",
            correlationId="case-1",
        )
    )

    assert wrong_actor.effect is None
    assert first.effect is None
    assert second.effect == FaultEffect.TIMEOUT
    assert second.delay_ms == 250
    assert exhausted.effect is None
    async with db() as session:
        count = await session.scalar(select(func.count()).select_from(FaultActivation))
    assert count == 1
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `docker compose run --rm test-runner pytest tests/contract/control/test_faults.py -q`  
Expected: FAIL because fault contracts and storage do not exist.

- [ ] **Step 3: Add exact fault request and decision contracts**

```python
# append to src/enterprise_twins/common/control/contracts.py
from enum import StrEnum
from typing import Any


class FaultPhase(StrEnum):
    BEFORE_VALIDATION = "before_validation"
    BEFORE_COMMIT = "before_commit"
    AFTER_COMMIT = "after_commit"
    READ = "read"
    EVENT_DELIVERY = "event_delivery"
    DOMAIN_COMPLETION = "domain_completion"


class FaultEffect(StrEnum):
    MALFORMED_TRANSPORT = "malformed_transport"
    UNAUTHENTICATED = "unauthenticated"
    RATE_LIMITED = "rate_limited"
    TEMPORARY_FAILURE = "temporary_failure"
    DELAY = "delay"
    TIMEOUT = "timeout"
    CONNECTION_LOSS = "connection_loss"
    MALFORMED_RESPONSE = "malformed_response"
    STALE_VERSION = "stale_version"
    TEMPORARY_ABSENCE = "temporary_absence"
    PAGINATION_CHANGE = "pagination_change"
    DUPLICATE = "duplicate"
    REORDER = "reorder"
    SUPPRESS = "suppress"
    RETRY = "retry"
    FAILED_REFUND = "failed_refund"
    DELAYED_SETTLEMENT = "delayed_settlement"
    BOUNCE = "bounce"
    DEFER = "defer"
    DROP = "drop"


class FaultRuleCreate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    rule_id: str = Field(alias="ruleId")
    target_service: str = Field(alias="targetService")
    operation: str
    phase: FaultPhase
    effect: FaultEffect
    actor_id: str | None = Field(default=None, alias="actorId")
    resource_id: str | None = Field(default=None, alias="resourceId")
    correlation_id: str | None = Field(default=None, alias="correlationId")
    request_hash: str | None = Field(default=None, alias="requestHash")
    occurrence: int = Field(default=1, ge=1)
    activation_count: int = Field(default=1, ge=1, alias="activationCount")
    delay_ms: int | None = Field(default=None, ge=0, alias="delayMs")
    response_data: dict[str, Any] = Field(default_factory=dict, alias="responseData")


class FaultProbe(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    target_service: str = Field(alias="targetService")
    operation: str
    phase: FaultPhase
    actor_id: str | None = Field(default=None, alias="actorId")
    resource_id: str | None = Field(default=None, alias="resourceId")
    correlation_id: str | None = Field(default=None, alias="correlationId")
    request_hash: str | None = Field(default=None, alias="requestHash")


class FaultDecision(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    rule_id: str | None = Field(default=None, alias="ruleId")
    effect: FaultEffect | None = None
    delay_ms: int | None = Field(default=None, alias="delayMs")
    response_data: dict[str, Any] = Field(default_factory=dict, alias="responseData")
```

- [ ] **Step 4: Add fault and activation tables**

```python
# append to src/enterprise_twins/services/control/models.py
from typing import Any

from sqlalchemy import String
from sqlalchemy.dialects.postgresql import JSONB

from enterprise_twins.common.db.base import ScenarioOwned


class FaultRule(ScenarioOwned, Base):
    __tablename__ = "fault_rules"
    rule_id: Mapped[str] = mapped_column(String(120), primary_key=True)
    target_service: Mapped[str] = mapped_column(String(80), index=True)
    operation: Mapped[str] = mapped_column(String(160), index=True)
    phase: Mapped[str] = mapped_column(String(40))
    effect: Mapped[str] = mapped_column(String(40))
    actor_id: Mapped[str | None] = mapped_column(String(128))
    resource_id: Mapped[str | None] = mapped_column(String(128))
    correlation_id: Mapped[str | None] = mapped_column(String(128))
    request_hash: Mapped[str | None] = mapped_column(String(64))
    occurrence: Mapped[int] = mapped_column(Integer, nullable=False)
    seen_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    remaining_count: Mapped[int] = mapped_column(Integer, nullable=False)
    delay_ms: Mapped[int | None] = mapped_column(Integer)
    response_data: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class FaultActivation(ScenarioOwned, Base):
    __tablename__ = "fault_activations"
    activation_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    rule_id: Mapped[str] = mapped_column(String(120), index=True)
    operation: Mapped[str] = mapped_column(String(160))
    correlation_id: Mapped[str | None] = mapped_column(String(128), index=True)
    phase: Mapped[str] = mapped_column(String(40))
    effect: Mapped[str] = mapped_column(String(40))
    activated_at: Mapped[datetime] = mapped_column(Timestamp, nullable=False)
```

- [ ] **Step 5: Implement deterministic matching and activation**

```python
# src/enterprise_twins/services/control/faults.py
from sqlalchemy import delete, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.contracts import FaultDecision, FaultEffect, FaultProbe, FaultRuleCreate
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.ids import new_id
from enterprise_twins.services.control.models import FaultActivation, FaultRule, VirtualClock


class FaultRepository:
    def __init__(self, factory: async_sessionmaker[AsyncSession]) -> None:
        self.factory = factory

    async def create(self, request: FaultRuleCreate) -> FaultRuleCreate:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1)
            if state is None or state.mode != "active":
                raise RuntimeError("scenario is not active")
            session.add(
                FaultRule(
                    rule_id=request.rule_id,
                    scenario_epoch=state.active_epoch,
                    target_service=request.target_service,
                    operation=request.operation,
                    phase=request.phase.value,
                    effect=request.effect.value,
                    actor_id=request.actor_id,
                    resource_id=request.resource_id,
                    correlation_id=request.correlation_id,
                    request_hash=request.request_hash,
                    occurrence=request.occurrence,
                    seen_count=0,
                    remaining_count=request.activation_count,
                    delay_ms=request.delay_ms,
                    response_data=request.response_data,
                )
            )
        return request

    async def evaluate(self, probe: FaultProbe) -> FaultDecision:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1)
            clock = await session.get(VirtualClock, 1)
            if state is None or clock is None:
                raise RuntimeError("control state is not initialised")
            rule = await session.scalar(
                select(FaultRule)
                .where(
                    FaultRule.scenario_epoch == state.active_epoch,
                    FaultRule.target_service == probe.target_service,
                    FaultRule.operation == probe.operation,
                    FaultRule.phase == probe.phase.value,
                    FaultRule.remaining_count > 0,
                    or_(FaultRule.actor_id.is_(None), FaultRule.actor_id == probe.actor_id),
                    or_(FaultRule.resource_id.is_(None), FaultRule.resource_id == probe.resource_id),
                    or_(FaultRule.correlation_id.is_(None), FaultRule.correlation_id == probe.correlation_id),
                    or_(FaultRule.request_hash.is_(None), FaultRule.request_hash == probe.request_hash),
                )
                .order_by(FaultRule.rule_id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if rule is None:
                return FaultDecision()
            rule.seen_count += 1
            if rule.seen_count < rule.occurrence:
                return FaultDecision()
            rule.remaining_count -= 1
            session.add(
                FaultActivation(
                    activation_id=new_id("flt"),
                    scenario_epoch=state.active_epoch,
                    rule_id=rule.rule_id,
                    operation=rule.operation,
                    correlation_id=probe.correlation_id,
                    phase=rule.phase,
                    effect=rule.effect,
                    activated_at=clock.now,
                )
            )
            return FaultDecision(
                ruleId=rule.rule_id,
                effect=FaultEffect(rule.effect),
                delayMs=rule.delay_ms,
                responseData=rule.response_data,
            )

    async def clear(self) -> None:
        async with self.factory.begin() as session:
            await session.execute(delete(FaultActivation))
            await session.execute(delete(FaultRule))

    async def list_activations(self) -> list[FaultActivation]:
        async with self.factory() as session:
            rows = await session.scalars(select(FaultActivation).order_by(FaultActivation.activation_id))
            return list(rows)
```

- [ ] **Step 6: Expose private fault routes and extend the client**

Add `fault_router(repository: FaultRepository, settings: ControlSettings) -> APIRouter` with these exact mappings:

```python
from typing import Annotated

from fastapi import APIRouter, Depends

from enterprise_twins.common.control.contracts import FaultDecision, FaultProbe, FaultRuleCreate
from enterprise_twins.common.control.auth import require_token
from enterprise_twins.services.control.settings import ControlSettings


def fault_router(repository: FaultRepository, settings: ControlSettings) -> APIRouter:
    router = APIRouter()
    ControllerAuth = Annotated[None, Depends(require_token(settings.controller_token))]
    TwinAuth = Annotated[None, Depends(require_token(settings.twin_token))]

    @router.post("/control/v1/faults", status_code=201)
    async def create_fault(request: FaultRuleCreate, _auth: ControllerAuth) -> FaultRuleCreate:
        return await repository.create(request)

    @router.delete("/control/v1/faults", status_code=204)
    async def clear_faults(_auth: ControllerAuth) -> None:
        await repository.clear()

    @router.post("/control/v1/faults/evaluate")
    async def evaluate_fault(request: FaultProbe, _auth: TwinAuth) -> FaultDecision:
        return await repository.evaluate(request)

    @router.get("/control/v1/fault-activations")
    async def fault_activations(_auth: ControllerAuth) -> list[dict[str, object]]:
        return [
            {
                "activationId": item.activation_id,
                "ruleId": item.rule_id,
                "operation": item.operation,
                "correlationId": item.correlation_id,
                "phase": item.phase,
                "effect": item.effect,
                "activatedAt": item.activated_at,
            }
            for item in await repository.list_activations()
        ]

    return router
```

Mount this router in `create_control_app`. Extend `ControlClient` with:

```python
async def evaluate_fault(self, probe: FaultProbe) -> FaultDecision:
    response = await self.client.post(
        f"{self.base_url}/control/v1/faults/evaluate",
        headers={"Authorization": f"Bearer {self.token}"},
        json=probe.model_dump(mode="json", by_alias=True),
    )
    response.raise_for_status()
    return FaultDecision.model_validate(response.json())
```

- [ ] **Step 7: Run focused tests and static checks**

Run:

```bash
docker compose build test-runner
docker compose run --rm test-runner pytest tests/contract/control/test_faults.py -q
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy
```

Expected: the occurrence test passes and static checks report no errors.

- [ ] **Step 8: Commit**

```bash
git add src/enterprise_twins/common/control src/enterprise_twins/services/control tests/contract/control
git commit -m "feat: add deterministic fault injection"
```

### Task 6: Epoch-staged reset protocol and operator CLI

**Files:**
- Modify: `src/enterprise_twins/common/control/contracts.py`
- Create: `src/enterprise_twins/common/control/participant.py`
- Modify: `src/enterprise_twins/services/control/models.py`
- Create: `src/enterprise_twins/services/control/reset.py`
- Modify: `src/enterprise_twins/services/control/settings.py`
- Modify: `src/enterprise_twins/services/control/app.py`
- Create: `src/enterprise_twins/services/control/cli.py`
- Create: `tests/unit/control/test_reset_coordinator.py`
- Create: `tests/contract/common/test_reset_participant.py`

**Interfaces:**
- Consumes: canonical digest, `ScenarioState`, platform records, private token dependency, Control clock, and fault repository.
- Produces: `ScenarioLoader` protocol; `ResetParticipant.prepare()`, `load()`, `commit()`, and `abort()`; admin-only reset app factory; `ResetCoordinator.reset(request: ResetRequest) -> ResetResult`; `POST /control/v1/reset`; `twins reset`, `twins time advance`, `twins faults apply`, and `twins status` commands.

- [ ] **Step 1: Write coordinator success and failure tests**

```python
# tests/unit/control/test_reset_coordinator.py
from datetime import UTC, datetime

import pytest

from enterprise_twins.common.control.contracts import ParticipantLoadRequest, ParticipantReport, ResetRequest
from enterprise_twins.services.control.reset import ResetCoordinator, ScenarioBundle


class Participant:
    def __init__(self, name: str, fail_on: str | None = None) -> None:
        self.name = name
        self.fail_on = fail_on
        self.calls: list[tuple[str, str]] = []

    async def prepare(self, epoch: str) -> None:
        self.calls.append(("prepare", epoch))

    async def load(self, request: ParticipantLoadRequest) -> ParticipantReport:
        self.calls.append(("load", request.scenario_epoch))
        if self.fail_on == "load":
            raise RuntimeError(f"{self.name} load failed")
        return ParticipantReport(
            service=self.name,
            schemaVersion="1",
            counts=request.payload["expectedCounts"],
            checksum=request.checksum,
        )

    async def commit(self, epoch: str) -> None:
        self.calls.append(("commit", epoch))

    async def abort(self, epoch: str) -> None:
        self.calls.append(("abort", epoch))


@pytest.mark.asyncio
async def test_reset_is_ordered_and_same_inputs_have_same_checksum() -> None:
    identity = Participant("identity")
    crm = Participant("crm")
    bundle = ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={
            "identity": {"expectedCounts": {"clients": 2}},
            "crm": {"expectedCounts": {"customers": 3}},
        },
    )
    coordinator = ResetCoordinator.for_test({"identity": identity, "crm": crm}, bundle)
    first = await coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1, randomSeed=7))
    second = await coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1, randomSeed=7))

    assert first.manifest_checksum == second.manifest_checksum
    assert first.random_seed == 7
    assert [name for name, _epoch in identity.calls[:3]] == ["prepare", "load", "commit"]
    assert [name for name, _epoch in crm.calls[:3]] == ["prepare", "load", "commit"]


@pytest.mark.asyncio
async def test_failed_load_aborts_every_participant_and_marks_estate_unhealthy() -> None:
    identity = Participant("identity")
    crm = Participant("crm", fail_on="load")
    bundle = ScenarioBundle(
        scenario_id="platform-contracts",
        version=1,
        initial_time=datetime(2026, 8, 19, 10, tzinfo=UTC),
        payloads={"identity": {"expectedCounts": {}}, "crm": {"expectedCounts": {}}},
    )
    coordinator = ResetCoordinator.for_test({"identity": identity, "crm": crm}, bundle)

    with pytest.raises(RuntimeError, match="crm load failed"):
        await coordinator.reset(ResetRequest(scenarioId="platform-contracts", version=1))

    assert identity.calls[-1][0] == "abort"
    assert crm.calls[-1][0] == "abort"
    assert coordinator.test_mode == "error"
```

- [ ] **Step 2: Write participant visibility and abort tests**

```python
# tests/contract/common/test_reset_participant.py
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.control.contracts import ParticipantLoadRequest
from enterprise_twins.common.control.participant import ResetParticipant, ScenarioLoader
from enterprise_twins.common.db.records import ScenarioState


class RecordingLoader(ScenarioLoader):
    def __init__(self) -> None:
        self.loaded: dict[str, dict[str, Any]] = {}

    async def load(self, session: AsyncSession, epoch: str, payload: dict[str, Any]) -> dict[str, object]:
        self.loaded[epoch] = payload
        return {"schemaVersion": "1", "counts": payload["expectedCounts"]}

    async def discard(self, session: AsyncSession, epoch: str) -> None:
        self.loaded.pop(epoch, None)


@pytest.mark.asyncio
async def test_staged_epoch_is_not_active_until_commit(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_old"))
    loader = RecordingLoader()
    participant = ResetParticipant(db, loader)
    payload = {"expectedCounts": {"records": 2}}

    await participant.prepare("epoch_new")
    report = await participant.load(
        ParticipantLoadRequest(
            scenarioEpoch="epoch_new",
            scenarioId="platform-contracts",
            scenarioVersion=1,
            randomSeed=7,
            payload=payload,
            checksum=sha256_hex(payload),
        )
    )
    async with db() as session:
        before = await session.get(ScenarioState, 1)
        assert before is not None
        assert before.active_epoch == "epoch_old"
        assert before.mode == "loaded"
    await participant.commit("epoch_new")
    async with db() as session:
        after = await session.get(ScenarioState, 1)
        assert after is not None
        assert after.active_epoch == "epoch_new"
        assert after.mode == "active"
    assert report.checksum == sha256_hex(payload)


@pytest.mark.asyncio
async def test_abort_discards_pending_epoch_and_leaves_service_unhealthy(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_old"))
    loader = RecordingLoader()
    participant = ResetParticipant(db, loader)
    await participant.prepare("epoch_new")
    await participant.abort("epoch_new")
    async with db() as session:
        state = await session.get(ScenarioState, 1)
        assert state is not None
        assert state.active_epoch == "epoch_old"
        assert state.mode == "error"
```

- [ ] **Step 3: Run both test files to verify they fail**

Run:

```bash
docker compose run --rm test-runner pytest tests/unit/control/test_reset_coordinator.py tests/contract/common/test_reset_participant.py -q
```

Expected: FAIL because reset contracts and implementations do not exist.

- [ ] **Step 4: Add reset contracts**

```python
# append to src/enterprise_twins/common/control/contracts.py
from typing import Any


class ResetRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    scenario_id: str = Field(alias="scenarioId")
    version: int = Field(ge=1)
    random_seed: int | None = Field(default=None, alias="randomSeed")


class ParticipantLoadRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    scenario_epoch: str = Field(alias="scenarioEpoch")
    scenario_id: str = Field(alias="scenarioId")
    scenario_version: int = Field(alias="scenarioVersion")
    random_seed: int = Field(alias="randomSeed")
    payload: dict[str, Any]
    checksum: str


class ParticipantReport(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    service: str
    schema_version: str = Field(alias="schemaVersion")
    counts: dict[str, int]
    aliases: dict[str, str] = Field(default_factory=dict)
    checksum: str


class ResetResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    scenario_id: str = Field(alias="scenarioId")
    version: int
    random_seed: int = Field(alias="randomSeed")
    scenario_epoch: str = Field(alias="scenarioEpoch")
    manifest_checksum: str = Field(alias="manifestChecksum")
    reports: list[ParticipantReport]
```

- [ ] **Step 5: Implement the generic reset participant and admin app**

```python
# src/enterprise_twins/common/control/participant.py
from typing import Any, Protocol

from fastapi import APIRouter, Depends, FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.control.auth import require_token
from enterprise_twins.common.control.contracts import ParticipantLoadRequest, ParticipantReport
from enterprise_twins.common.db.records import AuditRecord, IdempotencyRecord, OutboxRecord, ScenarioState
from enterprise_twins.common.http.errors import ApiError, ErrorCode


class ScenarioLoader(Protocol):
    async def load(
        self, session: AsyncSession, epoch: str, payload: dict[str, Any]
    ) -> dict[str, object]:
        raise NotImplementedError

    async def discard(self, session: AsyncSession, epoch: str) -> None:
        raise NotImplementedError


class ResetParticipant:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        loader: ScenarioLoader,
        service: str = "test",
    ) -> None:
        self.factory = factory
        self.loader = loader
        self.service = service

    async def database_ready(self) -> bool:
        try:
            async with self.factory() as session:
                await session.get(ScenarioState, 1)
        except Exception:
            return False
        return True

    async def prepare(self, epoch: str) -> None:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1, with_for_update=True)
            if state is None:
                state = ScenarioState(singleton_id=1, mode="uninitialised", active_epoch="none")
                session.add(state)
            if state.mode not in {"active", "error", "uninitialised"}:
                raise ApiError(ErrorCode.CONFLICT, "another reset is active", status_code=409)
            state.mode = "preparing"
            state.pending_epoch = epoch
            await session.execute(delete(IdempotencyRecord))
            await session.execute(delete(OutboxRecord))
            await session.execute(delete(AuditRecord))

    async def load(self, request: ParticipantLoadRequest) -> ParticipantReport:
        if sha256_hex(request.payload) != request.checksum:
            raise ApiError(ErrorCode.INVALID_REQUEST, "scenario payload checksum differs", status_code=422)
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1, with_for_update=True)
            if state is None or state.pending_epoch != request.scenario_epoch or state.mode != "preparing":
                raise ApiError(ErrorCode.CONFLICT, "participant is not prepared for this epoch", status_code=409)
            result = await self.loader.load(session, request.scenario_epoch, request.payload)
            state.mode = "loaded"
            state.scenario_id = request.scenario_id
            state.scenario_version = request.scenario_version
            state.random_seed = request.random_seed
            state.manifest_checksum = request.checksum
            return ParticipantReport(
                service=self.service,
                schemaVersion=str(result["schemaVersion"]),
                counts=dict(result["counts"]),
                aliases=dict(result.get("aliases", {})),
                checksum=request.checksum,
            )

    async def commit(self, epoch: str) -> None:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1, with_for_update=True)
            if state is None or state.pending_epoch != epoch or state.mode != "loaded":
                raise ApiError(ErrorCode.CONFLICT, "participant has not loaded this epoch", status_code=409)
            previous = state.active_epoch
            state.active_epoch = epoch
            state.pending_epoch = None
            state.mode = "active"
            if previous != "none":
                await self.loader.discard(session, previous)

    async def abort(self, epoch: str) -> None:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1, with_for_update=True)
            if state is None:
                return
            if state.pending_epoch == epoch:
                await self.loader.discard(session, epoch)
                state.pending_epoch = None
            if state.active_epoch == epoch or state.mode != "active":
                state.mode = "error"


def create_participant_app(name: str, participant: ResetParticipant, token: str) -> FastAPI:
    app = FastAPI(title=f"{name} reset participant", docs_url=None, redoc_url=None)
    router = APIRouter(prefix="/internal/v1/reset", dependencies=[Depends(require_token(token))])

    @router.post("/prepare", status_code=204)
    async def prepare(body: dict[str, str]) -> None:
        await participant.prepare(body["scenarioEpoch"])

    @router.post("/load")
    async def load(body: ParticipantLoadRequest) -> ParticipantReport:
        return await participant.load(body)

    @router.post("/commit", status_code=204)
    async def commit(body: dict[str, str]) -> None:
        await participant.commit(body["scenarioEpoch"])

    @router.post("/abort", status_code=204)
    async def abort(body: dict[str, str]) -> None:
        await participant.abort(body["scenarioEpoch"])

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "live"}

    @app.get("/health/ready")
    async def ready() -> JSONResponse:
        is_ready = await participant.database_ready()
        return JSONResponse(
            {"status": "ready" if is_ready else "not_ready"},
            status_code=200 if is_ready else 503,
        )

    app.include_router(router)
    return app
```

- [ ] **Step 6: Implement bundle loading and reset coordination**

```python
# core types and algorithm in src/enterprise_twins/services/control/reset.py
import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

import httpx

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.control.contracts import (
    ParticipantLoadRequest,
    ParticipantReport,
    ResetRequest,
    ResetResult,
)
from enterprise_twins.common.ids import new_id


@dataclass(frozen=True, slots=True)
class ScenarioBundle:
    scenario_id: str
    version: int
    initial_time: datetime
    payloads: dict[str, dict[str, Any]]

    @property
    def checksum(self) -> str:
        return sha256_hex(
            {
                "scenarioId": self.scenario_id,
                "version": self.version,
                "initialTime": self.initial_time,
                "payloads": self.payloads,
            }
        )


class ParticipantClient(Protocol):
    async def prepare(self, epoch: str) -> None:
        raise NotImplementedError

    async def load(self, request: ParticipantLoadRequest) -> ParticipantReport:
        raise NotImplementedError

    async def commit(self, epoch: str) -> None:
        raise NotImplementedError

    async def abort(self, epoch: str) -> None:
        raise NotImplementedError


BundleLoader = Callable[[str, int], ScenarioBundle]
BeginControl = Callable[[str, ScenarioBundle, int], Awaitable[None]]
CommitControl = Callable[[str, ScenarioBundle, int], Awaitable[None]]
FailControl = Callable[[str], Awaitable[None]]


class ResetCoordinator:
    def __init__(
        self,
        participants: dict[str, ParticipantClient],
        load_bundle: BundleLoader,
        begin_control: BeginControl,
        commit_control: CommitControl,
        fail_control: FailControl,
    ) -> None:
        self.participants = participants
        self.load_bundle = load_bundle
        self.begin_control = begin_control
        self.commit_control = commit_control
        self.fail_control = fail_control
        self.lock = asyncio.Lock()
        self.test_mode = "active"

    @classmethod
    def for_test(
        cls, participants: dict[str, ParticipantClient], bundle: ScenarioBundle
    ) -> "ResetCoordinator":
        async def no_action(*_args: object) -> None:
            return None

        coordinator = cls(participants, lambda _sid, _version: bundle, no_action, no_action, no_action)
        return coordinator

    async def reset(self, request: ResetRequest) -> ResetResult:
        async with self.lock:
            bundle = self.load_bundle(request.scenario_id, request.version)
            seed = request.random_seed if request.random_seed is not None else derive_seed(request.scenario_id, request.version)
            epoch = new_id("epoch")
            reports: list[ParticipantReport] = []
            await self.begin_control(epoch, bundle, seed)
            try:
                for participant in self.participants.values():
                    await participant.prepare(epoch)
                for name, participant in self.participants.items():
                    payload = bundle.payloads[name]
                    checksum = sha256_hex(payload)
                    report = await participant.load(
                        ParticipantLoadRequest(
                            scenarioEpoch=epoch,
                            scenarioId=bundle.scenario_id,
                            scenarioVersion=bundle.version,
                            randomSeed=seed,
                            payload=payload,
                            checksum=checksum,
                        )
                    )
                    if (
                        report.service != name
                        or report.checksum != checksum
                        or report.counts != payload["expectedCounts"]
                    ):
                        raise RuntimeError(f"{name} reset verification failed")
                    reports.append(report)
                for participant in self.participants.values():
                    await participant.commit(epoch)
                await self.commit_control(epoch, bundle, seed)
                self.test_mode = "active"
            except Exception:
                await asyncio.gather(
                    *(participant.abort(epoch) for participant in self.participants.values()),
                    return_exceptions=True,
                )
                await self.fail_control(epoch)
                self.test_mode = "error"
                raise
            return ResetResult(
                scenarioId=bundle.scenario_id,
                version=bundle.version,
                randomSeed=seed,
                scenarioEpoch=epoch,
                manifestChecksum=bundle.checksum,
                reports=reports,
            )


def derive_seed(scenario_id: str, version: int) -> int:
    digest = hashlib.sha256(f"{scenario_id}:{version}".encode()).digest()
    return int.from_bytes(digest[:8], signed=False)
```

Add these two concrete adapters to the same file:

```python
class HttpParticipantClient:
    def __init__(self, base_url: str, token: str, client: httpx.AsyncClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {token}"}
        self.client = client

    async def post(self, action: str, body: dict[str, object]) -> httpx.Response:
        response = await self.client.post(
            f"{self.base_url}/internal/v1/reset/{action}",
            headers=self.headers,
            json=body,
            timeout=5.0,
        )
        response.raise_for_status()
        return response

    async def prepare(self, epoch: str) -> None:
        await self.post("prepare", {"scenarioEpoch": epoch})

    async def load(self, request: ParticipantLoadRequest) -> ParticipantReport:
        response = await self.post("load", request.model_dump(mode="json", by_alias=True))
        return ParticipantReport.model_validate(response.json())

    async def commit(self, epoch: str) -> None:
        await self.post("commit", {"scenarioEpoch": epoch})

    async def abort(self, epoch: str) -> None:
        await self.post("abort", {"scenarioEpoch": epoch})


class DirectoryBundleLoader:
    def __init__(self, scenario_root: Path) -> None:
        self.scenario_root = scenario_root.resolve()

    def __call__(self, scenario_id: str, version: int) -> ScenarioBundle:
        if re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", scenario_id) is None:
            raise ValueError("scenario ID has invalid characters")
        directory = (self.scenario_root / scenario_id).resolve()
        if not directory.is_relative_to(self.scenario_root):
            raise ValueError("scenario path escapes the configured root")
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        if manifest["scenarioId"] != scenario_id or manifest["version"] != version:
            raise ValueError("scenario manifest ID or version differs")
        payloads: dict[str, dict[str, Any]] = {}
        for service, item in manifest["services"].items():
            filename = item["file"]
            path = (directory / filename).resolve()
            if not path.is_relative_to(directory):
                raise ValueError(f"scenario file for {service} escapes its directory")
            payload = json.loads(path.read_text(encoding="utf-8"))
            if sha256_hex(payload) != item["checksum"]:
                raise ValueError(f"scenario checksum differs for {service}")
            payloads[service] = payload
        initial = datetime.fromisoformat(manifest["initialTime"].replace("Z", "+00:00"))
        return ScenarioBundle(scenario_id, version, initial, payloads)
```

- [ ] **Step 7: Persist Control reset state and expose the endpoint**

Add `ResetRun` to `services/control/models.py`:

```python
class ResetRun(Base):
    __tablename__ = "reset_runs"
    reset_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    scenario_id: Mapped[str] = mapped_column(String(80), index=True)
    scenario_version: Mapped[int] = mapped_column(Integer, nullable=False)
    random_seed: Mapped[int] = mapped_column(BigInteger, nullable=False)
    scenario_epoch: Mapped[str] = mapped_column(String(64), unique=True)
    state: Mapped[str] = mapped_column(String(24), nullable=False)
    manifest_checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    error: Mapped[str | None] = mapped_column(String(500))
```

Import `BigInteger` alongside the existing SQLAlchemy types. Implement the
coordinator callbacks in `ControlResetStore`:

```python
class ControlResetStore:
    def __init__(self, factory: async_sessionmaker[AsyncSession]) -> None:
        self.factory = factory

    async def begin(self, epoch: str, bundle: ScenarioBundle, seed: int) -> None:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1, with_for_update=True)
            if state is None:
                state = ScenarioState(singleton_id=1, mode="uninitialised", active_epoch="none")
                session.add(state)
            state.mode = "preparing"
            state.pending_epoch = epoch
            await session.execute(delete(FaultActivation))
            await session.execute(delete(FaultRule))
            clock = await session.get(VirtualClock, 1)
            if clock is None:
                session.add(VirtualClock(singleton_id=1, now=bundle.initial_time))
            else:
                clock.now = bundle.initial_time
            session.add(
                ResetRun(
                    reset_id=new_id("rst"),
                    scenario_id=bundle.scenario_id,
                    scenario_version=bundle.version,
                    random_seed=seed,
                    scenario_epoch=epoch,
                    state="preparing",
                    manifest_checksum=bundle.checksum,
                )
            )

    async def commit(self, epoch: str, bundle: ScenarioBundle, seed: int) -> None:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1, with_for_update=True)
            if state is None or state.pending_epoch != epoch:
                raise RuntimeError("control reset epoch differs")
            state.active_epoch = epoch
            state.pending_epoch = None
            state.mode = "active"
            state.scenario_id = bundle.scenario_id
            state.scenario_version = bundle.version
            state.random_seed = seed
            state.manifest_checksum = bundle.checksum
            run = await session.scalar(select(ResetRun).where(ResetRun.scenario_epoch == epoch))
            if run is None:
                raise RuntimeError("control reset run is missing")
            run.state = "committed"

    async def fail(self, epoch: str) -> None:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1, with_for_update=True)
            if state is not None and state.pending_epoch == epoch:
                state.pending_epoch = None
                state.mode = "error"
            run = await session.scalar(select(ResetRun).where(ResetRun.scenario_epoch == epoch))
            if run is not None:
                run.state = "failed"
                run.error = "participant reset failed"
```

Import `delete`, `select`, `AsyncSession`, `async_sessionmaker`,
`new_id`, `ScenarioState`, `FaultActivation`, `FaultRule`, `ResetRun`, and
`VirtualClock` in `reset.py`.

Mount this exact controller route with the existing controller-token dependency:

```python
def reset_router(
    coordinator: ResetCoordinator,
    repository: ControlRepository,
    settings: ControlSettings,
) -> APIRouter:
    router = APIRouter()
    ControllerAuth = Annotated[None, Depends(require_token(settings.controller_token))]

    @router.post("/control/v1/reset")
    async def reset(request: ResetRequest, _auth: ControllerAuth) -> ResetResult:
        return await coordinator.reset(request)

    @router.get("/control/v1/status")
    async def status(_auth: ControllerAuth) -> dict[str, object]:
        state = await repository.state()
        return {
            "scenarioId": state.scenario_id,
            "version": state.scenario_version,
            "scenarioEpoch": state.active_epoch,
            "manifestChecksum": state.manifest_checksum,
            "mode": state.mode,
            "now": await repository.now(),
        }

    return router
```

Retain the Task 4 test factory and add this final environment-aware factory:

```python
def build_control_app(
    factory: async_sessionmaker[AsyncSession],
    settings: ControlSettings,
    coordinator: ResetCoordinator,
    lifespan: Callable[[FastAPI], AbstractAsyncContextManager[None]] | None = None,
) -> FastAPI:
    repository = ControlRepository(factory)
    faults = FaultRepository(factory)
    return create_app(
        "Twin Control",
        ("scenario:reset", "time:write", "faults:write", "diagnostics:read"),
        ControlStatus(repository),
        (
            control_router(repository, settings),
            fault_router(faults, settings),
            reset_router(coordinator, repository, settings),
        ),
        lifespan,
    )


def create_from_env() -> FastAPI:
    settings = ControlSettings()
    engine = make_engine(settings.database_url)
    factory = make_session_factory(engine)
    http_client = httpx.AsyncClient()
    participants = {
        name: HttpParticipantClient(url, settings.participant_token, http_client)
        for name, url in settings.participants.items()
    }
    store = ControlResetStore(factory)
    coordinator = ResetCoordinator(
        participants,
        DirectoryBundleLoader(settings.scenario_root),
        store.begin,
        store.commit,
        store.fail,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        repository = ControlRepository(factory)
        try:
            await repository.state()
        except RuntimeError:
            await coordinator.reset(
                ResetRequest(
                    scenarioId=settings.bootstrap_scenario,
                    version=settings.bootstrap_version,
                )
            )
        try:
            yield
        finally:
            await http_client.aclose()
            await engine.dispose()

    return build_control_app(factory, settings, coordinator, lifespan)
```

Import `Callable`, `AbstractAsyncContextManager`, `asynccontextmanager`,
`FastAPI`, `httpx`, database factories, routers, reset adapters, and settings
in `control/app.py`. Bootstrap runs only when Control has no scenario row, so
an application-container restart retains the active state.

- [ ] **Step 8: Add operator commands**

```python
# src/enterprise_twins/services/control/cli.py
import os

import httpx
import typer

app = typer.Typer(no_args_is_help=True)
time_app = typer.Typer()
faults_app = typer.Typer()
app.add_typer(time_app, name="time")
app.add_typer(faults_app, name="faults")


def client() -> httpx.Client:
    return httpx.Client(
        base_url=os.environ.get("TWINS_CONTROL_CLI_URL", "http://127.0.0.1:8000"),
        headers={"Authorization": f"Bearer {os.environ['TWINS_CONTROL_CONTROLLER_TOKEN']}"},
        timeout=10,
    )


@app.command()
def reset(scenario_id: str, version: int = 1, random_seed: int | None = None) -> None:
    response = client().post(
        "/control/v1/reset",
        json={"scenarioId": scenario_id, "version": version, "randomSeed": random_seed},
    )
    response.raise_for_status()
    typer.echo(response.text)


@time_app.command("advance")
def advance(duration: str) -> None:
    response = client().post("/control/v1/time/advance", json={"duration": duration})
    response.raise_for_status()
    typer.echo(response.text)


@app.command()
def status() -> None:
    response = client().get("/control/v1/status")
    response.raise_for_status()
    typer.echo(response.text)


@faults_app.command("apply")
def apply_fault(rule_file: str) -> None:
    response = client().post(
        "/control/v1/faults",
        content=Path(rule_file).read_bytes(),
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    typer.echo(response.text)
```

- [ ] **Step 9: Run reset tests and static checks**

Run:

```bash
docker compose build test-runner
docker compose run --rm test-runner pytest tests/unit/control/test_reset_coordinator.py tests/contract/common/test_reset_participant.py -q
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy
```

Expected: four reset tests pass and static checks report no errors.

- [ ] **Step 10: Commit**

```bash
git add src/enterprise_twins/common/control src/enterprise_twins/services/control tests/unit/control tests/contract/common
git commit -m "feat: add epoch-staged scenario reset"
```

### Task 7: Durable Event Relay and listener-free webhook worker

**Files:**
- Modify: `src/enterprise_twins/common/events/contracts.py`
- Create: `src/enterprise_twins/common/events/relay_client.py`
- Modify: `src/enterprise_twins/common/events/publisher.py`
- Create: `src/enterprise_twins/services/relay/models.py`
- Create: `src/enterprise_twins/services/relay/repository.py`
- Create: `src/enterprise_twins/services/relay/api.py`
- Create: `src/enterprise_twins/services/relay/delivery.py`
- Create: `src/enterprise_twins/services/relay/settings.py`
- Create: `src/enterprise_twins/services/relay/app.py`
- Create: `src/enterprise_twins/services/relay/admin_app.py`
- Create: `src/enterprise_twins/services/relay/scenario.py`
- Create: `tests/contract/relay/test_delivery.py`

**Interfaces:**
- Consumes: event envelope and outbox from Task 3, Control clock and fault client from Tasks 4 and 5, reset participant from Task 6.
- Produces: `WebhookSubscriptionCreate`, `WebhookSubscriptionCreated`, and redacted `WebhookSubscriptionView`; `RelayClient`; `OutboxDispatcher.run_once() -> int`; integration-only Relay API; `WebhookWorker.run_once() -> int`; Relay reset admin app. The worker process exposes no socket.

- [ ] **Step 1: Write signed delivery, redaction, and deduplication tests**

```python
# tests/contract/relay/test_delivery.py
import hashlib
import hmac
from datetime import UTC, datetime

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.contracts import FaultDecision
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.events.contracts import EventEnvelope, WebhookSubscriptionCreate
from enterprise_twins.services.relay.delivery import WebhookWorker
from enterprise_twins.services.relay.models import DeliveryAttempt
from enterprise_twins.services.relay.repository import RelayRepository


class Clock:
    async def now(self) -> datetime:
        return datetime(2026, 8, 19, 10, tzinfo=UTC)

    async def current_epoch(self) -> str:
        return "epoch_1"

    async def evaluate_fault(self, probe: object) -> FaultDecision:
        return FaultDecision()

@pytest.mark.asyncio
async def test_event_is_stored_once_and_delivered_with_valid_signature(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))
    repository = RelayRepository(db, allowed_targets={"webhook-receiver"})
    created = await repository.create_subscription(
        "crm",
        "person-support-1",
        "subscription-idem-1",
        WebhookSubscriptionCreate(
            eventTypes=["crm.note.created"],
            targetUrl="http://webhook-receiver:8080/events",
        ),
        datetime(2026, 8, 19, 10, tzinfo=UTC),
    )
    event = EventEnvelope(
        eventId="evt_1",
        eventType="crm.note.created",
        source="crm",
        subject="note/note_1",
        resourceVersion=1,
        correlationId="case-1",
        causationId="req-1",
        occurredAt="2026-08-19T10:00:00Z",
        recordedAt="2026-08-19T10:00:00Z",
        data={"noteId": "note_1"},
    )
    assert await repository.ingest(event) is True
    assert await repository.ingest(event) is False

    requests: list[httpx.Request] = []

    async def receive(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    worker = WebhookWorker(
        repository,
        Clock(),
        httpx.AsyncClient(transport=httpx.MockTransport(receive)),
    )
    assert await worker.run_once() == 1
    assert len(requests) == 1
    timestamp = requests[0].headers["X-Twin-Timestamp"]
    expected = hmac.new(
        created.secret.encode(), timestamp.encode() + b"." + requests[0].content, hashlib.sha256
    ).hexdigest()
    assert requests[0].headers["X-Twin-Signature"] == f"v1={expected}"
    assert requests[0].headers["X-Twin-Event-Id"] == "evt_1"
    listed = await repository.list_subscriptions("crm")
    assert listed[0].model_dump().get("secret") is None
    async with db() as session:
        attempts = await session.scalar(select(func.count()).select_from(DeliveryAttempt))
    assert attempts == 1


@pytest.mark.asyncio
async def test_target_outside_allowlist_is_rejected(
    db: async_sessionmaker[AsyncSession],
) -> None:
    repository = RelayRepository(db, allowed_targets={"webhook-receiver"})
    with pytest.raises(ValueError, match="target host is not allowed"):
        await repository.create_subscription(
            "crm",
            "person-support-1",
            "subscription-idem-2",
            WebhookSubscriptionCreate(eventTypes=["crm.note.created"], targetUrl="http://127.0.0.1/x"),
            datetime(2026, 8, 19, 10, tzinfo=UTC),
        )
```

- [ ] **Step 2: Run the Relay tests to verify they fail**

Run: `docker compose run --rm test-runner pytest tests/contract/relay/test_delivery.py -q`  
Expected: FAIL because Relay contracts and models do not exist.

- [ ] **Step 3: Add subscription and ingest contracts**

```python
# append to src/enterprise_twins/common/events/contracts.py
from pydantic import AnyHttpUrl


class WebhookSubscriptionCreate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    event_types: list[str] = Field(min_length=1, alias="eventTypes")
    target_url: AnyHttpUrl = Field(alias="targetUrl")


class WebhookSubscriptionView(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    subscription_id: str = Field(alias="subscriptionId")
    source: str
    event_types: list[str] = Field(alias="eventTypes")
    target_url: AnyHttpUrl = Field(alias="targetUrl")
    version: int


class WebhookSubscriptionCreated(WebhookSubscriptionView):
    secret: str
```

```python
# src/enterprise_twins/services/relay/settings.py
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RelaySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWINS_RELAY_", extra="ignore")
    database_url: str
    control_url: str = "http://control:8000"
    control_token: str = "twin-local-token"
    source_tokens: dict[str, str] = Field(default_factory=dict)
    allowed_targets: set[str] = Field(default_factory=set)
    participant_token: str = "participant-local-token"
```

- [ ] **Step 4: Add Relay persistence models**

```python
# src/enterprise_twins/services/relay/models.py
from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from enterprise_twins.common.db.base import Base, ScenarioOwned, Timestamp


class Subscription(ScenarioOwned, Base):
    __tablename__ = "relay_subscriptions"
    __table_args__ = (UniqueConstraint("scenario_epoch", "subscription_id"),)
    row_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    subscription_id: Mapped[str] = mapped_column(String(64), index=True)
    source: Mapped[str] = mapped_column(String(80), index=True)
    event_types: Mapped[list[str]] = mapped_column(ARRAY(String(160)))
    target_url: Mapped[str] = mapped_column(String(1000))
    signing_secret: Mapped[str] = mapped_column(String(200))
    version: Mapped[int] = mapped_column(Integer, default=1)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(Timestamp)


class SourceEvent(ScenarioOwned, Base):
    __tablename__ = "relay_source_events"
    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source: Mapped[str] = mapped_column(String(80), index=True)
    event_type: Mapped[str] = mapped_column(String(160), index=True)
    body_hash: Mapped[str] = mapped_column(String(64))
    envelope: Mapped[dict[str, Any]] = mapped_column(JSONB)


class Delivery(ScenarioOwned, Base):
    __tablename__ = "relay_deliveries"
    __table_args__ = (UniqueConstraint("event_id", "subscription_id"),)
    delivery_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(64), index=True)
    subscription_id: Mapped[str] = mapped_column(String(64), index=True)
    state: Mapped[str] = mapped_column(String(24), default="pending", index=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(Timestamp, index=True)
    lease_until: Mapped[datetime | None] = mapped_column(Timestamp, index=True)
    last_status: Mapped[int | None] = mapped_column(Integer)


class DeliveryAttempt(ScenarioOwned, Base):
    __tablename__ = "relay_delivery_attempts"
    attempt_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    delivery_id: Mapped[str] = mapped_column(String(64), index=True)
    attempted_at: Mapped[datetime] = mapped_column(Timestamp)
    response_status: Mapped[int | None] = mapped_column(Integer)
    outcome: Mapped[str] = mapped_column(String(40))
```

- [ ] **Step 5: Implement Relay repository rules**

Implement `RelayRepository` with these exact behaviours in one transaction per
method:

```python
import secrets
from datetime import datetime, timedelta

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.db.idempotency import (
    IdempotencyNamespace,
    StoredResponse,
    run_idempotent,
)
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.control.contracts import FaultDecision, FaultEffect
from enterprise_twins.common.events.contracts import (
    EventEnvelope,
    WebhookSubscriptionCreate,
    WebhookSubscriptionCreated,
    WebhookSubscriptionView,
)
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.common.ids import new_id
from enterprise_twins.services.relay.models import Delivery, DeliveryAttempt, SourceEvent, Subscription


class RelayRepository:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        allowed_targets: set[str],
    ) -> None:
        self.factory = factory
        self.allowed_targets = allowed_targets

    async def active_epoch(self, session: AsyncSession) -> str:
        state = await session.get(ScenarioState, 1)
        if state is None or state.mode != "active":
            raise RuntimeError("relay scenario is not active")
        return state.active_epoch

    async def create_subscription(
        self,
        source: str,
        caller_id: str,
        idempotency_key: str,
        request: WebhookSubscriptionCreate,
        now: datetime,
    ) -> WebhookSubscriptionCreated:
        host = request.target_url.host
        if host not in self.allowed_targets:
            raise ValueError("target host is not allowed")
        async with self.factory.begin() as session:
            epoch = await self.active_epoch(session)
            namespace = IdempotencyNamespace(
                "tenant_synthetic",
                caller_id,
                f"{source}.subscription.create",
                idempotency_key,
            )

            async def work() -> StoredResponse:
                item = Subscription(
                    row_id=new_id("subrow"),
                    subscription_id=new_id("sub"),
                    scenario_epoch=epoch,
                    source=source,
                    event_types=sorted(set(request.event_types)),
                    target_url=str(request.target_url),
                    signing_secret=secrets.token_urlsafe(32),
                    version=1,
                    active=True,
                    created_at=now,
                )
                session.add(item)
                return StoredResponse(
                    201,
                    WebhookSubscriptionCreated(
                        subscriptionId=item.subscription_id,
                        source=item.source,
                        eventTypes=item.event_types,
                        targetUrl=item.target_url,
                        version=item.version,
                        secret=item.signing_secret,
                    ).model_dump(mode="json", by_alias=True),
                    {},
                )

            result, _replayed = await run_idempotent(
                session,
                epoch,
                namespace,
                request.model_dump(mode="json", by_alias=True),
                work,
            )
            return WebhookSubscriptionCreated.model_validate(result.body)

    async def ingest(self, event: EventEnvelope) -> bool:
        body = event.model_dump(mode="json", by_alias=True)
        digest = sha256_hex(body)
        async with self.factory.begin() as session:
            epoch = await self.active_epoch(session)
            existing = await session.get(SourceEvent, event.event_id)
            if existing is not None:
                if existing.body_hash != digest:
                    raise ValueError("event ID was reused with changed data")
                return False
            session.add(
                SourceEvent(
                    event_id=event.event_id,
                    scenario_epoch=epoch,
                    source=event.source,
                    event_type=event.event_type,
                    body_hash=digest,
                    envelope=body,
                )
            )
            subscriptions = await session.scalars(
                select(Subscription).where(
                    Subscription.scenario_epoch == epoch,
                    Subscription.source == event.source,
                    Subscription.active.is_(True),
                    Subscription.event_types.any(event.event_type),
                )
            )
            for subscription in subscriptions:
                session.add(
                    Delivery(
                        delivery_id=new_id("dlv"),
                        scenario_epoch=epoch,
                        event_id=event.event_id,
                        subscription_id=subscription.subscription_id,
                        state="pending",
                        attempt_count=0,
                        next_attempt_at=event.occurred_at,
                    )
                )
            return True
```

Add these methods to `RelayRepository`:

```python
class RelayRepository:
    async def list_subscriptions(self, source: str) -> list[WebhookSubscriptionView]:
        async with self.factory() as session:
            epoch = await self.active_epoch(session)
            rows = await session.scalars(
                select(Subscription)
                .where(
                    Subscription.scenario_epoch == epoch,
                    Subscription.source == source,
                    Subscription.active.is_(True),
                )
                .order_by(Subscription.subscription_id)
            )
            return [
                WebhookSubscriptionView(
                    subscriptionId=row.subscription_id,
                    source=row.source,
                    eventTypes=row.event_types,
                    targetUrl=row.target_url,
                    version=row.version,
                )
                for row in rows
            ]

    async def delete_subscription(
        self,
        source: str,
        caller_id: str,
        idempotency_key: str,
        subscription_id: str,
        expected_version: int,
    ) -> None:
        async with self.factory.begin() as session:
            epoch = await self.active_epoch(session)
            namespace = IdempotencyNamespace(
                "tenant_synthetic",
                caller_id,
                f"{source}.subscription.delete",
                idempotency_key,
            )

            async def work() -> StoredResponse:
                row = await session.scalar(
                    select(Subscription)
                    .where(
                        Subscription.scenario_epoch == epoch,
                        Subscription.source == source,
                        Subscription.subscription_id == subscription_id,
                        Subscription.active.is_(True),
                    )
                    .with_for_update()
                )
                if row is None:
                    raise ApiError(ErrorCode.NOT_FOUND, "subscription was not found", status_code=404)
                if row.version != expected_version:
                    raise ApiError(ErrorCode.CONFLICT, "subscription version differs", status_code=409)
                row.active = False
                row.version += 1
                return StoredResponse(204, {}, {})

            await run_idempotent(
                session,
                epoch,
                namespace,
                {"subscriptionId": subscription_id, "expectedVersion": expected_version},
                work,
            )

    async def next_delivery(
        self,
        now: datetime,
    ) -> tuple[Delivery, Subscription, SourceEvent] | None:
        async with self.factory.begin() as session:
            epoch = await self.active_epoch(session)
            delivery = await session.scalar(
                select(Delivery)
                .where(
                    Delivery.scenario_epoch == epoch,
                    or_(
                        Delivery.state == "pending",
                        (Delivery.state == "in_flight") & (Delivery.lease_until <= now),
                    ),
                    Delivery.next_attempt_at <= now,
                )
                .order_by(Delivery.next_attempt_at, Delivery.delivery_id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if delivery is None:
                return None
            subscription = await session.scalar(
                select(Subscription).where(
                    Subscription.scenario_epoch == epoch,
                    Subscription.subscription_id == delivery.subscription_id,
                )
            )
            event = await session.get(SourceEvent, delivery.event_id)
            if subscription is None or event is None:
                raise RuntimeError("delivery source records are missing")
            delivery.state = "in_flight"
            delivery.attempt_count += 1
            delivery.lease_until = now + timedelta(seconds=30)
            return delivery, subscription, event

    async def finish_attempt(
        self,
        delivery_id: str,
        attempted_at: datetime,
        response_status: int | None,
        transport_error: str | None,
    ) -> None:
        async with self.factory.begin() as session:
            delivery = await session.get(Delivery, delivery_id, with_for_update=True)
            if delivery is None:
                raise RuntimeError("delivery is missing")
            success = response_status is not None and 200 <= response_status < 300
            session.add(
                DeliveryAttempt(
                    attempt_id=new_id("att"),
                    scenario_epoch=delivery.scenario_epoch,
                    delivery_id=delivery.delivery_id,
                    attempted_at=attempted_at,
                    response_status=response_status,
                    outcome="acknowledged" if success else transport_error or "http_failure",
                )
            )
            delivery.last_status = response_status
            delivery.lease_until = None
            if success:
                delivery.state = "delivered"
            elif delivery.state != "delivered":
                delivery.state = "pending"
                delay = min(2 ** delivery.attempt_count, 300)
                delivery.next_attempt_at = attempted_at + timedelta(seconds=delay)
```

Import `timedelta`, `ApiError`, and `ErrorCode` in `repository.py`.

- [ ] **Step 6: Implement signature and one-at-a-time delivery**

```python
# core of src/enterprise_twins/services/relay/delivery.py
import hashlib
import hmac
from datetime import datetime
from typing import Protocol

import httpx

from enterprise_twins.common.canonical import canonical_json
from enterprise_twins.common.control.contracts import FaultDecision, FaultEffect, FaultPhase, FaultProbe
from enterprise_twins.services.relay.repository import RelayRepository


def signature(secret: str, timestamp: str, body: bytes) -> str:
    digest = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    return f"v1={digest}"


class RelayControl(Protocol):
    async def now(self) -> datetime:
        raise NotImplementedError

    async def evaluate_fault(self, probe: FaultProbe) -> FaultDecision:
        raise NotImplementedError


class WebhookWorker:
    def __init__(
        self,
        repository: RelayRepository,
        control: RelayControl,
        client: httpx.AsyncClient,
    ) -> None:
        self.repository = repository
        self.control = control
        self.client = client

    async def run_once(self) -> int:
        now = await self.control.now()
        candidate = await self.repository.next_delivery(now)
        if candidate is None:
            return 0
        delivery, subscription, event = candidate
        body = canonical_json(event.envelope)
        timestamp = now.isoformat().replace("+00:00", "Z")
        decision = await self.control.evaluate_fault(
            FaultProbe(
                targetService="event-relay",
                operation="webhook.deliver",
                phase=FaultPhase.EVENT_DELIVERY,
                resourceId=delivery.delivery_id,
                correlationId=event.envelope["correlationId"],
            )
        )
        if decision.effect is not None:
            handled = await self.repository.apply_delivery_fault(delivery, decision, now)
            if handled:
                return 1
        copies = 2 if decision.effect == FaultEffect.DUPLICATE else 1
        for _copy in range(copies):
            try:
                response = await self.client.post(
                    subscription.target_url,
                    content=body,
                    headers={
                        "Content-Type": "application/json",
                        "X-Twin-Event-Id": event.event_id,
                        "X-Twin-Timestamp": timestamp,
                        "X-Twin-Signature": signature(subscription.signing_secret, timestamp, body),
                    },
                    timeout=2.0,
                )
                await self.repository.finish_attempt(
                    delivery.delivery_id,
                    now,
                    response.status_code,
                    None,
                )
            except httpx.HTTPError as error:
                await self.repository.finish_attempt(
                    delivery.delivery_id,
                    now,
                    None,
                    type(error).__name__,
                )
        return 1
```

Add this repository method. Duplicate returns control to the worker, which
makes two requests with the same event ID and body:

```python
class RelayRepository:
    async def apply_delivery_fault(
        self,
        delivery: Delivery,
        decision: FaultDecision,
        now: datetime,
    ) -> bool:
        if decision.effect == FaultEffect.DUPLICATE:
            return False
        async with self.factory.begin() as session:
            current = await session.get(Delivery, delivery.delivery_id, with_for_update=True)
            if current is None:
                raise RuntimeError("delivery is missing")
            current.lease_until = None
            if decision.effect == FaultEffect.DELAY:
                current.state = "pending"
                current.next_attempt_at = now + timedelta(milliseconds=decision.delay_ms or 1)
                outcome = "injected_delay"
            elif decision.effect == FaultEffect.SUPPRESS:
                current.state = "suppressed"
                outcome = "injected_suppress"
            elif decision.effect == FaultEffect.RETRY:
                current.state = "pending"
                current.next_attempt_at = now + timedelta(seconds=1)
                outcome = "injected_retry"
            elif decision.effect == FaultEffect.REORDER:
                current.state = "pending"
                current.next_attempt_at = now + timedelta(seconds=2)
                outcome = "injected_reorder"
            else:
                return False
            session.add(
                DeliveryAttempt(
                    attempt_id=new_id("att"),
                    scenario_epoch=current.scenario_epoch,
                    delivery_id=current.delivery_id,
                    attempted_at=now,
                    response_status=None,
                    outcome=outcome,
                )
            )
        return True
```

- [ ] **Step 7: Add integration API, clients, source dispatcher, and reset loader**

Create the integration router. It checks the source-specific bearer token and
the event's source field:

```python
# core of src/enterprise_twins/services/relay/api.py
import hmac
from typing import Annotated

from fastapi import APIRouter, Header

from enterprise_twins.common.control.client import ControlClient
from enterprise_twins.common.events.contracts import (
    EventEnvelope,
    WebhookSubscriptionCreate,
    WebhookSubscriptionCreated,
    WebhookSubscriptionView,
)
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.relay.repository import RelayRepository
from enterprise_twins.services.relay.settings import RelaySettings


def relay_router(
    repository: RelayRepository,
    control: ControlClient,
    settings: RelaySettings,
) -> APIRouter:
    router = APIRouter(prefix="/internal/v1")

    def authorise(source: str, authorization: str | None) -> None:
        expected = settings.source_tokens.get(source, "")
        supplied = authorization.removeprefix("Bearer ") if authorization else ""
        if not expected or not hmac.compare_digest(expected, supplied):
            raise ApiError(ErrorCode.UNAUTHENTICATED, "invalid source credential", status_code=401)

    @router.post("/sources/{source}/subscriptions", status_code=201)
    async def create_subscription(
        source: str,
        body: WebhookSubscriptionCreate,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
        caller_id: Annotated[str, Header(alias="X-Caller-Id")],
        authorization: Annotated[str | None, Header()] = None,
    ) -> WebhookSubscriptionCreated:
        authorise(source, authorization)
        try:
            return await repository.create_subscription(
                source,
                caller_id,
                idempotency_key,
                body,
                await control.now(),
            )
        except ValueError as error:
            raise ApiError(ErrorCode.INVALID_REQUEST, str(error), status_code=422) from error

    @router.get("/sources/{source}/subscriptions")
    async def list_subscriptions(
        source: str,
        authorization: Annotated[str | None, Header()] = None,
    ) -> list[WebhookSubscriptionView]:
        authorise(source, authorization)
        return await repository.list_subscriptions(source)

    @router.delete("/sources/{source}/subscriptions/{subscription_id}", status_code=204)
    async def delete_subscription(
        source: str,
        subscription_id: str,
        if_match: Annotated[str, Header(alias="If-Match")],
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
        caller_id: Annotated[str, Header(alias="X-Caller-Id")],
        authorization: Annotated[str | None, Header()] = None,
    ) -> None:
        authorise(source, authorization)
        try:
            expected_version = int(if_match.strip('"'))
        except ValueError as error:
            raise ApiError(ErrorCode.INVALID_REQUEST, "If-Match is invalid", status_code=422) from error
        await repository.delete_subscription(
            source,
            caller_id,
            idempotency_key,
            subscription_id,
            expected_version,
        )

    @router.post("/events", status_code=202)
    async def ingest(
        event: EventEnvelope,
        authorization: Annotated[str | None, Header()] = None,
    ) -> dict[str, bool]:
        authorise(event.source, authorization)
        return {"accepted": await repository.ingest(event)}

    return router
```

Create the source-side client:

```python
# src/enterprise_twins/common/events/relay_client.py
import httpx

from enterprise_twins.common.events.contracts import (
    EventEnvelope,
    WebhookSubscriptionCreate,
    WebhookSubscriptionCreated,
    WebhookSubscriptionView,
)
from enterprise_twins.common.http.errors import ApiError, ErrorCode


def require_relay_success(response: httpx.Response) -> None:
    if response.status_code < 400:
        return
    try:
        error = response.json()["error"]
        code = ErrorCode(error["code"])
        message = str(error["message"])
        retryable = bool(error.get("retryable", False))
        details = dict(error.get("details", {}))
    except (KeyError, TypeError, ValueError):
        code = ErrorCode.TEMPORARILY_UNAVAILABLE
        message = "event relay returned an invalid error response"
        retryable = response.status_code >= 500
        details = {}
    raise ApiError(
        code,
        message,
        status_code=response.status_code,
        retryable=retryable,
        details=details,
    )


class RelayClient:
    def __init__(self, base_url: str, source: str, token: str, client: httpx.AsyncClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.source = source
        self.headers = {"Authorization": f"Bearer {token}"}
        self.client = client

    async def ingest(self, event: EventEnvelope) -> None:
        response = await self.client.post(
            f"{self.base_url}/internal/v1/events",
            headers=self.headers,
            json=event.model_dump(mode="json", by_alias=True),
            timeout=2.0,
        )
        require_relay_success(response)

    async def create_subscription(
        self,
        caller_id: str,
        idempotency_key: str,
        request: WebhookSubscriptionCreate,
    ) -> WebhookSubscriptionCreated:
        response = await self.client.post(
            f"{self.base_url}/internal/v1/sources/{self.source}/subscriptions",
            headers=self.headers
            | {"X-Caller-Id": caller_id, "Idempotency-Key": idempotency_key},
            json=request.model_dump(mode="json", by_alias=True),
            timeout=2.0,
        )
        require_relay_success(response)
        return WebhookSubscriptionCreated.model_validate(response.json())

    async def list_subscriptions(self) -> list[WebhookSubscriptionView]:
        response = await self.client.get(
            f"{self.base_url}/internal/v1/sources/{self.source}/subscriptions",
            headers=self.headers,
            timeout=2.0,
        )
        require_relay_success(response)
        return [WebhookSubscriptionView.model_validate(item) for item in response.json()]

    async def delete_subscription(
        self,
        caller_id: str,
        idempotency_key: str,
        subscription_id: str,
        version: int,
    ) -> None:
        response = await self.client.delete(
            f"{self.base_url}/internal/v1/sources/{self.source}/subscriptions/{subscription_id}",
            headers=self.headers
            | {
                "X-Caller-Id": caller_id,
                "Idempotency-Key": idempotency_key,
                "If-Match": f'"{version}"',
            },
            timeout=2.0,
        )
        require_relay_success(response)
```

Complete the source dispatcher in `common/events/publisher.py`:

```python
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

import httpx

from enterprise_twins.common.events.relay_client import RelayClient
from enterprise_twins.common.http.errors import ApiError


class OutboxDispatcher:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        relay: RelayClient,
    ) -> None:
        self.factory = factory
        self.relay = relay

    async def run_once(self) -> int:
        async with self.factory.begin() as session:
            state = await session.get(ScenarioState, 1)
            if state is None or state.mode != "active":
                return 0
            record = await session.scalar(
                select(OutboxRecord)
                .where(
                    OutboxRecord.scenario_epoch == state.active_epoch,
                    OutboxRecord.published.is_(False),
                )
                .order_by(OutboxRecord.event_id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if record is None:
                return 0
            record.publish_attempts += 1
            try:
                await self.relay.ingest(EventEnvelope.model_validate(record.envelope))
            except (ApiError, httpx.HTTPError):
                return 0
            record.published = True
            record.published_at = datetime.now(UTC)
            return 1
```

Implement Relay reset support:

```python
# src/enterprise_twins/services/relay/scenario.py
from typing import Any

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from enterprise_twins.services.relay.models import (
    Delivery,
    DeliveryAttempt,
    SourceEvent,
    Subscription,
)


class RelayScenarioLoader:
    async def load(
        self,
        session: AsyncSession,
        epoch: str,
        payload: dict[str, Any],
    ) -> dict[str, object]:
        subscriptions = payload.get("subscriptions", [])
        if subscriptions:
            raise ValueError("platform-contracts Relay seed must start without subscriptions")
        return {
            "schemaVersion": payload["schemaVersion"],
            "counts": {"subscriptions": 0, "events": 0, "deliveries": 0, "attempts": 0},
            "aliases": payload.get("aliases", {}),
        }

    async def discard(self, session: AsyncSession, epoch: str) -> None:
        await session.execute(delete(DeliveryAttempt).where(DeliveryAttempt.scenario_epoch == epoch))
        await session.execute(delete(Delivery).where(Delivery.scenario_epoch == epoch))
        await session.execute(delete(SourceEvent).where(SourceEvent.scenario_epoch == epoch))
        await session.execute(delete(Subscription).where(Subscription.scenario_epoch == epoch))
```

```python
# src/enterprise_twins/services/relay/admin_app.py
from fastapi import FastAPI

from enterprise_twins.common.control.participant import ResetParticipant, create_participant_app
from enterprise_twins.common.db.runtime import make_engine, make_session_factory
from enterprise_twins.services.relay.scenario import RelayScenarioLoader
from enterprise_twins.services.relay.settings import RelaySettings


def create_from_env() -> FastAPI:
    settings = RelaySettings()
    factory = make_session_factory(make_engine(settings.database_url))
    participant = ResetParticipant(factory, RelayScenarioLoader(), "relay")
    return create_participant_app("Event Relay", participant, settings.participant_token)
```

Create the integration API factory:

```python
# core of src/enterprise_twins/services/relay/app.py
class RelayStatus:
    def __init__(self, factory: async_sessionmaker[AsyncSession]) -> None:
        self.factory = factory

    async def state(self) -> ScenarioState:
        async with self.factory() as session:
            state = await session.get(ScenarioState, 1)
            if state is None:
                raise RuntimeError("Relay scenario is not initialised")
            return state

    async def current_epoch(self) -> str:
        return (await self.state()).active_epoch

    async def readiness(self) -> tuple[bool, dict[str, str]]:
        try:
            state = await self.state()
        except RuntimeError:
            return False, {"database": "not_ready", "scenario": "uninitialised"}
        return state.mode == "active", {"database": "ready", "scenario": state.mode}


def create_from_env() -> FastAPI:
    settings = RelaySettings()
    engine = make_engine(settings.database_url)
    factory = make_session_factory(engine)
    http_client = httpx.AsyncClient()
    control = ControlClient(settings.control_url, settings.control_token, http_client)
    repository = RelayRepository(factory, settings.allowed_targets)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        try:
            yield
        finally:
            await http_client.aclose()
            await engine.dispose()

    return create_app(
        "Event Relay integration API",
        (),
        RelayStatus(factory),
        (relay_router(repository, control, settings),),
        lifespan,
    )
```

Add `async main()` to `delivery.py`. It creates the same settings, database,
HTTPX, Control client, repository, and worker, then loops as follows without
starting Uvicorn or binding a port. Import `SQLAlchemyError` from
`sqlalchemy.exc`:

```python
async def main() -> None:
    settings = RelaySettings()
    engine = make_engine(settings.database_url)
    factory = make_session_factory(engine)
    async with httpx.AsyncClient() as client:
        control = ControlClient(settings.control_url, settings.control_token, client)
        worker = WebhookWorker(
            RelayRepository(factory, settings.allowed_targets),
            control,
            client,
        )
        try:
            while True:
                try:
                    processed = await worker.run_once()
                except (RuntimeError, httpx.HTTPError, SQLAlchemyError):
                    processed = 0
                if processed == 0:
                    await asyncio.sleep(0.05)
        finally:
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 8: Run Relay tests and quality checks**

Run:

```bash
docker compose build test-runner
docker compose run --rm test-runner pytest tests/contract/relay/test_delivery.py -q
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy
```

Expected: two Relay tests pass, one signed request is observed, and static checks report no errors.

- [ ] **Step 9: Commit**

```bash
git add src/enterprise_twins/common/events src/enterprise_twins/services/relay tests/contract/relay
git commit -m "feat: add durable signed webhook relay"
```

### Task 8: Stateful Identity twin and shared bearer verification

**Files:**
- Create: `src/enterprise_twins/common/auth/claims.py`
- Create: `src/enterprise_twins/common/auth/verifier.py`
- Create: `src/enterprise_twins/common/auth/audit.py`
- Create: `src/enterprise_twins/services/identity/models.py`
- Create: `src/enterprise_twins/services/identity/secrets.py`
- Create: `src/enterprise_twins/services/identity/issuer.py`
- Create: `src/enterprise_twins/services/identity/repository.py`
- Create: `src/enterprise_twins/services/identity/api.py`
- Create: `src/enterprise_twins/services/identity/settings.py`
- Create: `src/enterprise_twins/services/identity/scenario.py`
- Create: `src/enterprise_twins/services/identity/app.py`
- Create: `src/enterprise_twins/services/identity/admin_app.py`
- Create: `tests/contract/identity/test_identity.py`
- Create: `tests/unit/auth/test_verifier.py`

**Interfaces:**
- Consumes: Control time and fault evaluation, service database records, Event Relay client and outbox dispatcher, reset participant.
- Produces: OIDC metadata, JWKS, OAuth client-credentials token endpoint, `/v1/me`, authenticated capabilities and webhook-subscription routes; `Principal`; `JwtVerifier.verify(token: str) -> Principal`; `require_scopes(*required: str)` dependency.

- [ ] **Step 1: Write Identity public-contract tests**

```python
# tests/contract/identity/test_identity.py
from datetime import UTC, datetime

import jwt
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.contracts import FaultDecision
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.services.identity.app import create_identity_app
from enterprise_twins.services.identity.models import IdentityClient
from enterprise_twins.services.identity.secrets import digest_secret
from enterprise_twins.services.identity.settings import IdentitySettings


class Clock:
    async def now(self) -> datetime:
        return datetime(2026, 8, 19, 10, tzinfo=UTC)

    async def current_epoch(self) -> str:
        return "epoch_1"

    async def evaluate_fault(self, probe: object) -> FaultDecision:
        return FaultDecision()


@pytest.mark.asyncio
async def test_client_credentials_jwks_and_self_view(
    db: async_sessionmaker[AsyncSession],
) -> None:
    settings = IdentitySettings(
        database_url="postgresql+asyncpg://unused",
        issuer="http://identity:8000",
        audience="enterprise-twins",
        signing_seed="identity-test-signing-seed",
        secret_pepper="identity-test-pepper",
        token_ttl_seconds=600,
    )
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))
        session.add(
            IdentityClient(
                row_id="irow_1",
                scenario_epoch="epoch_1",
                client_id="support-agent",
                secret_digest=digest_secret("support-agent", "support-secret", settings.secret_pepper),
                subject="person-support-1",
                actor_type="human",
                role="support_agent",
                scopes=["crm:read", "crm:notes:write"],
                tenant_id="tenant_synthetic",
                active=True,
                version=1,
            )
        )
    app = create_identity_app(db, settings, Clock())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://identity:8000") as client:
        metadata = await client.get("/.well-known/openid-configuration")
        keys = await client.get("/.well-known/jwks.json")
        token_response = await client.post(
            "/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "support-agent",
                "client_secret": "support-secret",
                "scope": "crm:read crm:notes:write",
            },
        )
        token = token_response.json()["access_token"]
        me = await client.get(
            "/v1/me",
            headers={"Authorization": f"Bearer {token}", "X-Correlation-Id": "case-1"},
        )

    assert metadata.json()["jwks_uri"] == "http://identity:8000/.well-known/jwks.json"
    assert keys.json()["keys"][0]["kty"] == "OKP"
    assert token_response.status_code == 200
    unverified = jwt.decode(token, options={"verify_signature": False})
    assert unverified["scope"] == "crm:notes:write crm:read"
    assert unverified["tenant"] == "tenant_synthetic"
    assert unverified["scenario_epoch"] == "epoch_1"
    assert me.json()["subject"] == "person-support-1"
    assert me.json()["role"] == "support_agent"


@pytest.mark.asyncio
async def test_wrong_secret_and_ungranted_scope_are_denied(
    identity_client: AsyncClient,
) -> None:
    wrong = await identity_client.post(
        "/oauth/token",
        data={"grant_type": "client_credentials", "client_id": "support-agent", "client_secret": "wrong"},
    )
    excessive = await identity_client.post(
        "/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "support-agent",
            "client_secret": "support-secret",
            "scope": "risk:restricted:read",
        },
    )
    assert wrong.status_code == 401
    assert excessive.status_code == 403
```

Add this fixture beside the test and import `AsyncIterator` and
`pytest_asyncio`:

```python
@pytest_asyncio.fixture
async def identity_client(
    db: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncClient]:
    settings = IdentitySettings(
        database_url="postgresql+asyncpg://unused",
        issuer="http://identity:8000",
        audience="enterprise-twins",
        signing_seed="identity-test-signing-seed",
        secret_pepper="identity-test-pepper",
        token_ttl_seconds=600,
    )
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))
        session.add(
            IdentityClient(
                row_id="irow_fixture",
                scenario_epoch="epoch_1",
                client_id="support-agent",
                secret_digest=digest_secret(
                    "support-agent", "support-secret", settings.secret_pepper
                ),
                subject="person-support-1",
                actor_type="human",
                role="support_agent",
                scopes=["crm:read", "crm:notes:write"],
                tenant_id="tenant_synthetic",
                active=True,
                version=1,
            )
        )
    app = create_identity_app(db, settings, Clock())
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://identity:8000",
    ) as client:
        yield client
```

- [ ] **Step 2: Run Identity tests to verify they fail**

Run: `docker compose run --rm test-runner pytest tests/contract/identity/test_identity.py -q`  
Expected: FAIL because Identity modules do not exist.

- [ ] **Step 3: Add client model and deterministic synthetic-secret digest**

```python
# src/enterprise_twins/services/identity/models.py
from sqlalchemy import Boolean, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from enterprise_twins.common.db.base import Base, ScenarioOwned


class IdentityClient(ScenarioOwned, Base):
    __tablename__ = "identity_clients"
    __table_args__ = (UniqueConstraint("scenario_epoch", "client_id"),)
    row_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    client_id: Mapped[str] = mapped_column(String(120), index=True)
    secret_digest: Mapped[str] = mapped_column(String(128))
    subject: Mapped[str] = mapped_column(String(128))
    actor_type: Mapped[str] = mapped_column(String(24))
    role: Mapped[str] = mapped_column(String(80))
    scopes: Mapped[list[str]] = mapped_column(ARRAY(String(160)))
    tenant_id: Mapped[str] = mapped_column(String(80))
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    version: Mapped[int] = mapped_column(Integer, default=1)
```

```python
# src/enterprise_twins/services/identity/secrets.py
import hashlib
import hmac


def digest_secret(client_id: str, secret: str, pepper: str) -> str:
    salt = hashlib.sha256(f"{pepper}:{client_id}".encode()).digest()[:16]
    value = hashlib.scrypt(secret.encode(), salt=salt, n=2**14, r=8, p=1, dklen=32)
    return value.hex()


def secret_matches(client_id: str, supplied: str, pepper: str, expected: str) -> bool:
    return hmac.compare_digest(digest_secret(client_id, supplied, pepper), expected)
```

```python
# src/enterprise_twins/services/identity/settings.py
from pydantic_settings import BaseSettings, SettingsConfigDict


class IdentitySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWINS_IDENTITY_", extra="ignore")
    database_url: str
    issuer: str = "http://identity:8000"
    audience: str = "enterprise-twins"
    signing_seed: str = "identity-test-signing-seed"
    secret_pepper: str
    token_ttl_seconds: int = 600
    control_url: str = "http://control:8000"
    control_token: str = "twin-local-token"
    relay_url: str = "http://event-relay-api:8000"
    relay_token: str = "identity-relay-local-token"
    participant_token: str = "participant-local-token"
```

- [ ] **Step 4: Implement Ed25519 issue and verification contracts**

```python
# src/enterprise_twins/common/auth/claims.py
from dataclasses import dataclass

from enterprise_twins.common.http.errors import ApiError, ErrorCode


@dataclass(frozen=True, slots=True)
class Principal:
    subject: str
    actor_type: str
    role: str
    scopes: frozenset[str]
    tenant_id: str
    token_id: str
    scenario_epoch: str

    def require(self, *required: str) -> None:
        missing = sorted(set(required) - self.scopes)
        if missing:
            raise ApiError(
                ErrorCode.FORBIDDEN,
                "required scope is missing",
                status_code=403,
                details={"requiredScopes": missing},
            )
```

```python
# key methods in src/enterprise_twins/services/identity/issuer.py
import base64
import hashlib
from datetime import datetime, timedelta

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from enterprise_twins.common.ids import new_id


def b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


class TokenIssuer:
    def __init__(self, issuer: str, audience: str, signing_seed: str, ttl_seconds: int) -> None:
        seed = hashlib.sha256(signing_seed.encode()).digest()
        self.private_key = Ed25519PrivateKey.from_private_bytes(seed)
        public = self.private_key.public_key().public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw,
        )
        self.kid = hashlib.sha256(public).hexdigest()[:16]
        self.public_jwk = {"kty": "OKP", "crv": "Ed25519", "use": "sig", "alg": "EdDSA", "kid": self.kid, "x": b64url(public)}
        self.issuer = issuer
        self.audience = audience
        self.ttl = timedelta(seconds=ttl_seconds)

    def issue(
        self,
        client: IdentityClient,
        scopes: list[str],
        now: datetime,
        scenario_epoch: str,
    ) -> tuple[str, str]:
        token_id = new_id("tok")
        claims = {
            "iss": self.issuer,
            "aud": self.audience,
            "sub": client.subject,
            "actor_type": client.actor_type,
            "role": client.role,
            "scope": " ".join(sorted(scopes)),
            "tenant": client.tenant_id,
            "scenario_epoch": scenario_epoch,
            "jti": token_id,
            "iat": int(now.timestamp()),
            "exp": int((now + self.ttl).timestamp()),
        }
        return jwt.encode(claims, self.private_key, algorithm="EdDSA", headers={"kid": self.kid}), token_id
```

```python
# src/enterprise_twins/common/auth/verifier.py
from datetime import datetime
from typing import Any, Protocol

import httpx
import jwt

from enterprise_twins.common.auth.claims import Principal
from enterprise_twins.common.http.errors import ApiError, ErrorCode


class TokenClock(Protocol):
    async def now(self) -> datetime:
        raise NotImplementedError

    async def current_epoch(self) -> str:
        raise NotImplementedError


class JwtVerifier:
    def __init__(
        self,
        issuer: str,
        audience: str,
        jwks_url: str,
        clock: TokenClock,
        client: httpx.AsyncClient,
    ) -> None:
        self.issuer = issuer
        self.audience = audience
        self.jwks_url = jwks_url
        self.clock = clock
        self.client = client
        self.keys: dict[str, jwt.PyJWK] = {}

    async def refresh(self) -> None:
        response = await self.client.get(self.jwks_url, timeout=2.0)
        response.raise_for_status()
        self.keys = {item["kid"]: jwt.PyJWK.from_dict(item) for item in response.json()["keys"]}

    async def verify(self, token: str) -> Principal:
        try:
            header = jwt.get_unverified_header(token)
            if header["kid"] not in self.keys:
                await self.refresh()
            key = self.keys[header["kid"]]
            claims: dict[str, Any] = jwt.decode(
                token,
                key.key,
                algorithms=["EdDSA"],
                audience=self.audience,
                issuer=self.issuer,
                options={
                    "verify_exp": False,
                    "require": ["exp", "iat", "jti", "sub", "scenario_epoch"],
                },
            )
            now: datetime = await self.clock.now()
            if int(claims["exp"]) <= int(now.timestamp()):
                raise jwt.ExpiredSignatureError
            if claims["scenario_epoch"] != await self.clock.current_epoch():
                raise jwt.InvalidTokenError("token belongs to another scenario epoch")
        except (KeyError, jwt.PyJWTError) as error:
            raise ApiError(ErrorCode.UNAUTHENTICATED, "bearer token is invalid", status_code=401) from error
        return Principal(
            subject=claims["sub"],
            actor_type=claims["actor_type"],
            role=claims["role"],
            scopes=frozenset(claims["scope"].split()),
            tenant_id=claims["tenant"],
            token_id=claims["jti"],
            scenario_epoch=claims["scenario_epoch"],
        )
```

Add the bearer dependency to the same file:

```python
from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import Depends, Header


class BearerAuthenticator:
    def __init__(
        self,
        verifier: JwtVerifier,
        recorder: "AuthDecisionRecorder | None" = None,
    ) -> None:
        self.verifier = verifier
        self.recorder = recorder

    async def authenticate(
        self,
        authorization: Annotated[str | None, Header()] = None,
    ) -> Principal:
        if authorization is None or not authorization.startswith("Bearer "):
            if self.recorder is not None:
                await self.recorder.record(None, (), False)
            raise ApiError(ErrorCode.UNAUTHENTICATED, "bearer token is required", status_code=401)
        try:
            principal = await self.verifier.verify(authorization.removeprefix("Bearer "))
        except ApiError:
            if self.recorder is not None:
                await self.recorder.record(None, (), False)
            raise
        if self.recorder is not None:
            await self.recorder.record(principal, (), True)
        return principal


class AuthDecisionRecorder(Protocol):
    async def record(
        self,
        principal: Principal | None,
        required_scopes: Sequence[str],
        allowed: bool,
    ) -> None:
        raise NotImplementedError


def require_scopes(
    authenticator: BearerAuthenticator,
    *required: str,
) -> Callable[[Principal], Awaitable[Principal]]:
    async def dependency(
        principal: Annotated[Principal, Depends(authenticator.authenticate)],
    ) -> Principal:
        try:
            principal.require(*required)
        except ApiError:
            if authenticator.recorder is not None:
                await authenticator.recorder.record(principal, required, False)
            raise
        if authenticator.recorder is not None:
            await authenticator.recorder.record(principal, required, True)
        return principal

    return dependency
```

```python
# src/enterprise_twins/common/auth/audit.py
from collections.abc import Sequence

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.auth.claims import Principal
from enterprise_twins.common.auth.verifier import TokenClock
from enterprise_twins.common.events.publisher import record_audit
from enterprise_twins.common.http.context import current_request


class DatabaseAuthDecisionRecorder:
    def __init__(self, service: str, factory: async_sessionmaker[AsyncSession], control: TokenClock) -> None:
        self.service = service
        self.factory = factory
        self.control = control

    async def record(
        self,
        principal: Principal | None,
        required_scopes: Sequence[str],
        allowed: bool,
    ) -> None:
        now = await self.control.now()
        epoch = await self.control.current_epoch()
        context = current_request.get()
        correlation_id = context.correlation_id if context else "uncorrelated"
        actor_id = principal.subject if principal is not None else "anonymous"
        async with self.factory.begin() as session:
            record_audit(
                session,
                epoch=epoch,
                action=f"{self.service}.authorisation.{'allowed' if allowed else 'denied'}",
                resource_type="api_request",
                resource_id=context.request_id if context else "unknown",
                actor_id=actor_id,
                correlation_id=correlation_id,
                occurred_at=now,
                details={"requiredScopes": sorted(required_scopes)},
            )
```

- [ ] **Step 5: Implement Identity repository, API, event write, and scenario loader**

Implement the repository and response type:

```python
# src/enterprise_twins/services/identity/repository.py
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.control.contracts import (
    FaultDecision,
    FaultEffect,
    FaultPhase,
    FaultProbe,
)
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.events.publisher import record_audit, record_event
from enterprise_twins.common.http.context import current_request
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.common.ids import new_id
from enterprise_twins.services.identity.issuer import TokenIssuer
from enterprise_twins.services.identity.models import IdentityClient
from enterprise_twins.services.identity.secrets import secret_matches
from enterprise_twins.services.identity.settings import IdentitySettings


class IdentityControl(Protocol):
    async def now(self) -> datetime:
        raise NotImplementedError

    async def current_epoch(self) -> str:
        raise NotImplementedError

    async def evaluate_fault(self, probe: FaultProbe) -> FaultDecision:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class TokenResult:
    access_token: str
    token_id: str
    scopes: list[str]
    expires_in: int


class IdentityRepository:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        settings: IdentitySettings,
        issuer: TokenIssuer,
        control: IdentityControl,
    ) -> None:
        self.factory = factory
        self.settings = settings
        self.issuer = issuer
        self.control = control

    async def record_denial(self, client_id: str, now: datetime, epoch: str) -> None:
        context = current_request.get()
        correlation_id = context.correlation_id if context else new_id("corr")
        async with self.factory.begin() as session:
            record_audit(
                session,
                epoch=epoch,
                action="identity.authentication.denied",
                resource_type="identity_client",
                resource_id=client_id,
                actor_id=client_id,
                correlation_id=correlation_id,
                occurred_at=now,
                details={"reason": "invalid_client_credentials"},
            )

    async def authenticate(
        self,
        client_id: str,
        secret: str,
        requested_scopes: list[str],
    ) -> TokenResult:
        decision = await self.control.evaluate_fault(
            FaultProbe(
                targetService="identity",
                operation="identity.token.issue",
                phase=FaultPhase.BEFORE_COMMIT,
                actorId=client_id,
            )
        )
        if decision.effect == FaultEffect.RATE_LIMITED:
            raise ApiError(ErrorCode.RATE_LIMITED, "token endpoint is rate limited", status_code=429, retryable=True)
        if decision.effect in {FaultEffect.TEMPORARY_FAILURE, FaultEffect.TIMEOUT}:
            raise ApiError(
                ErrorCode.TEMPORARILY_UNAVAILABLE,
                "token endpoint is temporarily unavailable",
                status_code=503,
                retryable=True,
            )
        now = await self.control.now()
        epoch = await self.control.current_epoch()
        async with self.factory.begin() as session:
            client = await session.scalar(
                select(IdentityClient).where(
                    IdentityClient.scenario_epoch == epoch,
                    IdentityClient.client_id == client_id,
                    IdentityClient.active.is_(True),
                )
            )
            if client is None or not secret_matches(
                client_id,
                secret,
                self.settings.secret_pepper,
                client.secret_digest if client is not None else "0" * 64,
            ):
                await self.record_denial(client_id, now, epoch)
                raise ApiError(ErrorCode.UNAUTHENTICATED, "client credentials are invalid", status_code=401)
            scopes = sorted(set(requested_scopes or client.scopes))
            if not set(scopes).issubset(client.scopes):
                raise ApiError(ErrorCode.FORBIDDEN, "requested scope is not granted", status_code=403)
            token, token_id = self.issuer.issue(client, scopes, now, epoch)
            context = current_request.get()
            correlation_id = context.correlation_id if context else token_id
            request_id = context.request_id if context else token_id
            record_audit(
                session,
                epoch=epoch,
                action="identity.token.issued",
                resource_type="identity_client",
                resource_id=client.client_id,
                actor_id=client.subject,
                correlation_id=correlation_id,
                occurred_at=now,
                details={"role": client.role, "tokenId": token_id},
            )
            record_event(
                session,
                epoch=epoch,
                event_type="identity.token.issued",
                source="identity",
                subject=f"identity/{client.subject}",
                resource_version=client.version,
                correlation_id=correlation_id,
                causation_id=request_id,
                occurred_at=now,
                data={"subject": client.subject, "role": client.role, "tokenId": token_id},
            )
            return TokenResult(token, token_id, scopes, self.settings.token_ttl_seconds)
```

The fallback digest value keeps `secret_matches` on the same scrypt path when
the client ID is absent, so absent IDs and wrong secrets use the same check.

Implement the public router:

```python
# core of src/enterprise_twins/services/identity/api.py
from typing import Annotated

from fastapi import APIRouter, Depends, Form, Header


def identity_router(
    repository: IdentityRepository,
    issuer: TokenIssuer,
    settings: IdentitySettings,
    authenticator: BearerAuthenticator,
    relay: RelayClient | None,
) -> APIRouter:
    router = APIRouter()
    AnyPrincipal = Annotated[Principal, Depends(authenticator.authenticate)]
    WebhookPrincipal = Annotated[Principal, Depends(require_scopes(authenticator, "webhooks:manage"))]

    @router.get("/.well-known/openid-configuration")
    async def metadata() -> dict[str, object]:
        return {
            "issuer": settings.issuer,
            "token_endpoint": f"{settings.issuer}/oauth/token",
            "jwks_uri": f"{settings.issuer}/.well-known/jwks.json",
            "grant_types_supported": ["client_credentials"],
            "token_endpoint_auth_methods_supported": ["client_secret_post"],
            "scopes_supported": [],
        }

    @router.get("/.well-known/jwks.json")
    async def jwks() -> dict[str, object]:
        return {"keys": [issuer.public_jwk]}

    @router.post("/oauth/token")
    async def token(
        grant_type: Annotated[str, Form()],
        client_id: Annotated[str, Form()],
        client_secret: Annotated[str, Form()],
        scope: Annotated[str, Form()] = "",
    ) -> dict[str, object]:
        if grant_type != "client_credentials":
            raise ApiError(ErrorCode.INVALID_REQUEST, "grant_type is not supported", status_code=422)
        result = await repository.authenticate(client_id, client_secret, scope.split())
        return {
            "access_token": result.access_token,
            "token_type": "bearer",
            "expires_in": result.expires_in,
            "scope": " ".join(result.scopes),
        }

    @router.get("/v1/me")
    async def me(principal: AnyPrincipal) -> dict[str, object]:
        return {
            "subject": principal.subject,
            "actorType": principal.actor_type,
            "role": principal.role,
            "scopes": sorted(principal.scopes),
            "tenant": principal.tenant_id,
            "tokenId": principal.token_id,
        }

    @router.get("/v1/capabilities")
    async def capabilities(_principal: AnyPrincipal) -> dict[str, object]:
        return {"service": "identity", "capabilities": ["tokens:issue", "webhooks:manage"]}

    @router.post("/v1/webhook-subscriptions", status_code=201)
    async def create_subscription(
        body: WebhookSubscriptionCreate,
        _principal: WebhookPrincipal,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> WebhookSubscriptionCreated:
        if relay is None:
            raise ApiError(ErrorCode.TEMPORARILY_UNAVAILABLE, "event relay is unavailable", status_code=503, retryable=True)
        return await relay.create_subscription(
            _principal.subject,
            idempotency_key,
            body,
        )

    @router.get("/v1/webhook-subscriptions")
    async def list_subscriptions(_principal: WebhookPrincipal) -> list[WebhookSubscriptionView]:
        if relay is None:
            raise ApiError(ErrorCode.TEMPORARILY_UNAVAILABLE, "event relay is unavailable", status_code=503, retryable=True)
        return await relay.list_subscriptions()

    @router.delete("/v1/webhook-subscriptions/{subscription_id}", status_code=204)
    async def delete_subscription(
        subscription_id: str,
        _principal: WebhookPrincipal,
        if_match: Annotated[str, Header(alias="If-Match")],
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> None:
        if relay is None:
            raise ApiError(ErrorCode.TEMPORARILY_UNAVAILABLE, "event relay is unavailable", status_code=503, retryable=True)
        await relay.delete_subscription(
            _principal.subject,
            idempotency_key,
            subscription_id,
            int(if_match.strip('"')),
        )

    return router
```

The internal Relay create request carries the caller and public idempotency
key. Relay persists `identity.subscription.create` in its database with the
subscription, so an uncertain source-to-Relay response can be retried without
creating a second subscription. Apply the same rule to delete with operation
`identity.subscription.delete` and replay status 204.

Implement the reset loader:

```python
# src/enterprise_twins/services/identity/scenario.py
from typing import Any

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from enterprise_twins.common.ids import new_id
from enterprise_twins.services.identity.models import IdentityClient
from enterprise_twins.services.identity.secrets import digest_secret


class IdentityScenarioLoader:
    def __init__(self, pepper: str) -> None:
        self.pepper = pepper

    async def load(
        self,
        session: AsyncSession,
        epoch: str,
        payload: dict[str, Any],
    ) -> dict[str, object]:
        clients = payload["clients"]
        client_ids = [item["clientId"] for item in clients]
        if len(client_ids) != len(set(client_ids)):
            raise ValueError("identity client IDs must be unique")
        for item in clients:
            session.add(
                IdentityClient(
                    row_id=new_id("irow"),
                    scenario_epoch=epoch,
                    client_id=item["clientId"],
                    secret_digest=digest_secret(item["clientId"], item["clientSecret"], self.pepper),
                    subject=item["subject"],
                    actor_type=item["actorType"],
                    role=item["role"],
                    scopes=sorted(set(item["scopes"])),
                    tenant_id=item["tenantId"],
                    active=True,
                    version=1,
                )
            )
        return {
            "schemaVersion": payload["schemaVersion"],
            "counts": {"clients": len(clients)},
            "aliases": payload.get("aliases", {}),
        }

    async def discard(self, session: AsyncSession, epoch: str) -> None:
        await session.execute(delete(IdentityClient).where(IdentityClient.scenario_epoch == epoch))
```

Create the public and admin factories:

```python
# core of src/enterprise_twins/services/identity/app.py
import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress

import httpx
import jwt
from fastapi import FastAPI
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class IdentityStatus:
    def __init__(self, factory: async_sessionmaker[AsyncSession]) -> None:
        self.factory = factory

    async def state(self) -> ScenarioState:
        async with self.factory() as session:
            state = await session.get(ScenarioState, 1)
            if state is None:
                raise RuntimeError("identity scenario is not initialised")
            return state

    async def current_epoch(self) -> str:
        return (await self.state()).active_epoch

    async def readiness(self) -> tuple[bool, dict[str, str]]:
        try:
            state = await self.state()
        except RuntimeError:
            return False, {"database": "not_ready", "scenario": "uninitialised"}
        return state.mode == "active", {"database": "ready", "scenario": state.mode}


def create_identity_app(
    factory: async_sessionmaker[AsyncSession],
    settings: IdentitySettings,
    control: IdentityControl,
    relay: RelayClient | None = None,
    verification_http: httpx.AsyncClient | None = None,
    lifespan: Callable[[FastAPI], AbstractAsyncContextManager[None]] | None = None,
) -> FastAPI:
    issuer = TokenIssuer(
        settings.issuer,
        settings.audience,
        settings.signing_seed,
        settings.token_ttl_seconds,
    )
    jwt_http = verification_http or httpx.AsyncClient()
    verifier = JwtVerifier(
        settings.issuer,
        settings.audience,
        f"{settings.issuer}/.well-known/jwks.json",
        control,
        jwt_http,
    )
    verifier.keys[issuer.kid] = jwt.PyJWK.from_dict(issuer.public_jwk)
    authenticator = BearerAuthenticator(
        verifier,
        DatabaseAuthDecisionRecorder("identity", factory, control),
    )
    repository = IdentityRepository(factory, settings, issuer, control)
    return create_app(
        "Identity twin",
        ("tokens:issue", "webhooks:manage"),
        IdentityStatus(factory),
        (identity_router(repository, issuer, settings, authenticator, relay),),
        lifespan,
    )
```

Add this environment factory to the same file:

```python
def create_from_env() -> FastAPI:
    settings = IdentitySettings()
    engine = make_engine(settings.database_url)
    factory = make_session_factory(engine)
    http_client = httpx.AsyncClient()
    control = ControlClient(settings.control_url, settings.control_token, http_client)
    relay = RelayClient(settings.relay_url, "identity", settings.relay_token, http_client)
    dispatcher = OutboxDispatcher(factory, relay)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        async def dispatch() -> None:
            while True:
                try:
                    await dispatcher.run_once()
                except SQLAlchemyError:
                    pass
                await asyncio.sleep(0.05)

        task = asyncio.create_task(dispatch())
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            await http_client.aclose()
            await engine.dispose()

    return create_identity_app(
        factory,
        settings,
        control,
        relay,
        http_client,
        lifespan,
    )
```

The test factory leaves Relay disabled and still writes durable outbox records.

```python
# src/enterprise_twins/services/identity/admin_app.py
from fastapi import FastAPI

from enterprise_twins.common.control.participant import ResetParticipant, create_participant_app
from enterprise_twins.common.db.runtime import make_engine, make_session_factory
from enterprise_twins.services.identity.scenario import IdentityScenarioLoader
from enterprise_twins.services.identity.settings import IdentitySettings


def create_from_env() -> FastAPI:
    settings = IdentitySettings()
    factory = make_session_factory(make_engine(settings.database_url))
    participant = ResetParticipant(
        factory,
        IdentityScenarioLoader(settings.secret_pepper),
        "identity",
    )
    return create_participant_app("Identity", participant, settings.participant_token)
```

- [ ] **Step 6: Add verifier unit cases**

```python
# tests/unit/auth/test_verifier.py
import pytest

from enterprise_twins.common.auth.claims import Principal
from enterprise_twins.common.http.errors import ApiError


def test_principal_reports_exact_missing_scopes() -> None:
    principal = Principal(
        subject="person-1",
        actor_type="human",
        role="support_agent",
        scopes=frozenset({"crm:read"}),
        tenant_id="tenant_synthetic",
        token_id="tok_1",
        scenario_epoch="epoch_1",
    )
    with pytest.raises(ApiError) as raised:
        principal.require("crm:read", "crm:notes:write")
    assert raised.value.status_code == 403
    assert raised.value.details == {"requiredScopes": ["crm:notes:write"]}
```

- [ ] **Step 7: Run Identity tests and quality checks**

Run:

```bash
docker compose build test-runner
docker compose run --rm test-runner pytest tests/contract/identity tests/unit/auth -q
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy
```

Expected: Identity and scope tests pass; decoded tokens have the fixed claims; static checks report no errors.

- [ ] **Step 8: Commit**

```bash
git add src/enterprise_twins/common/auth src/enterprise_twins/services/identity tests/contract/identity tests/unit/auth
git commit -m "feat: add stateful identity twin"
```

### Task 9: Stateful CRM twin with exact search and append-only notes

**Files:**
- Create: `src/enterprise_twins/services/crm/models.py`
- Create: `src/enterprise_twins/services/crm/schemas.py`
- Create: `src/enterprise_twins/services/crm/repository.py`
- Create: `src/enterprise_twins/services/crm/service.py`
- Create: `src/enterprise_twins/services/crm/api.py`
- Create: `src/enterprise_twins/services/crm/settings.py`
- Create: `src/enterprise_twins/services/crm/scenario.py`
- Create: `src/enterprise_twins/services/crm/app.py`
- Create: `src/enterprise_twins/services/crm/admin_app.py`
- Create: `tests/contract/crm/conftest.py`
- Create: `tests/contract/crm/test_customers.py`
- Create: `tests/contract/crm/test_notes.py`

**Interfaces:**
- Consumes: shared bearer verifier, common HTTP and persistence contracts, Control client, Relay client, reset participant.
- Produces: customer exact-search, customer read, note list, and idempotent note-create APIs; `crm.note.created` events; CRM reset loader and admin app.

- [ ] **Step 1: Write exact-search and ambiguity tests**

```python
# tests/contract/crm/test_customers.py
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_exact_search_returns_zero_one_or_many_without_ranking(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
) -> None:
    unique = await crm_client.get(
        "/v1/customers",
        params={"email": "alex.unique@example.test"},
        headers=support_headers,
    )
    ambiguous = await crm_client.get(
        "/v1/customers",
        params={"email": "shared@example.test"},
        headers=support_headers,
    )
    absent = await crm_client.get(
        "/v1/customers",
        params={"externalReference": "missing"},
        headers=support_headers,
    )

    assert [item["customerId"] for item in unique.json()["items"]] == ["cus_unique"]
    assert [item["customerId"] for item in ambiguous.json()["items"]] == ["cus_ambiguous_a", "cus_ambiguous_b"]
    assert absent.json()["items"] == []
```

Add these fixtures in `tests/contract/crm/conftest.py`. They use the real token
issuer and verifier against an in-process JWKS transport:

```python
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.auth.verifier import JwtVerifier
from enterprise_twins.common.control.contracts import FaultDecision
from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.services.crm.app import create_crm_app
from enterprise_twins.services.crm.models import Customer
from enterprise_twins.services.crm.settings import CrmSettings
from enterprise_twins.services.identity.issuer import TokenIssuer
from enterprise_twins.services.identity.models import IdentityClient


class Control:
    async def now(self) -> datetime:
        return datetime(2026, 8, 19, 10, tzinfo=UTC)

    async def current_epoch(self) -> str:
        return "epoch_1"

    async def evaluate_fault(self, probe: object) -> FaultDecision:
        return FaultDecision()


@dataclass
class Harness:
    client: AsyncClient
    support_headers: dict[str, str]
    read_only_headers: dict[str, str]


@pytest_asyncio.fixture
async def crm_harness(
    db: async_sessionmaker[AsyncSession],
) -> AsyncIterator[Harness]:
    customers = [
        ("cus_unique", "Alex Unique", "alex.unique@example.test", "ext-unique", "LOY-1001"),
        ("cus_ambiguous_a", "Sam Shared A", "shared@example.test", "ext-shared-a", "LOY-2001"),
        ("cus_ambiguous_b", "Sam Shared B", "shared@example.test", "ext-shared-b", "LOY-2002"),
    ]
    async with db.begin() as session:
        session.add(ScenarioState(singleton_id=1, mode="active", active_epoch="epoch_1"))
        for index, (customer_id, name, email, reference, loyalty) in enumerate(customers):
            session.add(
                Customer(
                    row_id=f"crow_{index}",
                    scenario_epoch="epoch_1",
                    customer_id=customer_id,
                    display_name=name,
                    primary_email=email,
                    external_reference=reference,
                    account_status="active",
                    contact_methods=[{"type": "email", "value": email, "primary": True}],
                    external_identifiers={"loyalty": loyalty},
                    version=1,
                )
            )
    issuer = TokenIssuer(
        "http://identity:8000",
        "enterprise-twins",
        "identity-test-signing-seed",
        600,
    )

    def client_record(subject: str, scopes: list[str], role: str) -> IdentityClient:
        return IdentityClient(
            row_id=f"irow_{subject}",
            scenario_epoch="epoch_1",
            client_id=subject,
            secret_digest="not-used",
            subject=subject,
            actor_type="service",
            role=role,
            scopes=scopes,
            tenant_id="tenant_synthetic",
            active=True,
            version=1,
        )

    now = await Control().now()
    support_token, _ = issuer.issue(
        client_record("person-support-1", ["crm:read", "crm:notes:write"], "support_agent"),
        ["crm:read", "crm:notes:write"],
        now,
        "epoch_1",
    )
    read_only_token, _ = issuer.issue(
        client_record("service-evaluator-1", ["crm:read"], "evaluator_service"),
        ["crm:read"],
        now,
        "epoch_1",
    )

    async def jwks(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"keys": [issuer.public_jwk]})

    verification_client = httpx.AsyncClient(transport=httpx.MockTransport(jwks))
    verifier = JwtVerifier(
        "http://identity:8000",
        "enterprise-twins",
        "http://identity:8000/.well-known/jwks.json",
        Control(),
        verification_client,
    )
    settings = CrmSettings(
        database_url="postgresql+asyncpg://unused",
        cursor_secret="crm-test-cursor",
    )
    app = create_crm_app(db, settings, Control(), verifier, relay=None)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://crm:8000") as client:
        yield Harness(
            client,
            {
                "Authorization": f"Bearer {support_token}",
                "X-Correlation-Id": "case-crm-test",
            },
            {
                "Authorization": f"Bearer {read_only_token}",
                "X-Correlation-Id": "case-crm-read-only",
            },
        )
    await verification_client.aclose()


@pytest.fixture
def crm_client(crm_harness: Harness) -> AsyncClient:
    return crm_harness.client


@pytest.fixture
def support_headers(crm_harness: Harness) -> dict[str, str]:
    return crm_harness.support_headers


@pytest.fixture
def read_only_headers(crm_harness: Harness) -> dict[str, str]:
    return crm_harness.read_only_headers
```

- [ ] **Step 2: Write note concurrency and idempotency tests**

```python
# tests/contract/crm/test_notes.py
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_note_create_replays_and_changed_payload_conflicts(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
) -> None:
    headers = support_headers | {"Idempotency-Key": "note-idem-1", "If-Match": '"1"'}
    first = await crm_client.post(
        "/v1/customers/cus_unique/notes",
        headers=headers,
        json={"body": "Customer prefers email", "association": "account"},
    )
    replay = await crm_client.post(
        "/v1/customers/cus_unique/notes",
        headers=headers,
        json={"body": "Customer prefers email", "association": "account"},
    )
    changed = await crm_client.post(
        "/v1/customers/cus_unique/notes",
        headers=headers,
        json={"body": "Changed body", "association": "account"},
    )
    notes = await crm_client.get(
        "/v1/customers/cus_unique/notes",
        headers=support_headers,
    )

    assert first.status_code == 201
    assert replay.status_code == 201
    assert first.json() == replay.json()
    assert replay.headers["Idempotency-Replayed"] == "true"
    assert changed.status_code == 409
    assert [note["body"] for note in notes.json()["items"]] == ["Customer prefers email"]


@pytest.mark.asyncio
async def test_stale_customer_version_and_missing_scope_are_denied(
    crm_client: AsyncClient,
    support_headers: dict[str, str],
    read_only_headers: dict[str, str],
) -> None:
    stale = await crm_client.post(
        "/v1/customers/cus_unique/notes",
        headers=support_headers | {"Idempotency-Key": "note-idem-2", "If-Match": '"0"'},
        json={"body": "stale", "association": "account"},
    )
    forbidden = await crm_client.post(
        "/v1/customers/cus_unique/notes",
        headers=read_only_headers | {"Idempotency-Key": "note-idem-3", "If-Match": '"1"'},
        json={"body": "forbidden", "association": "account"},
    )
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "conflict"
    assert forbidden.status_code == 403
```

- [ ] **Step 3: Run CRM tests to verify they fail**

Run: `docker compose run --rm test-runner pytest tests/contract/crm -q`  
Expected: FAIL because CRM modules and fixtures do not exist.

- [ ] **Step 4: Add customer and note models**

```python
# src/enterprise_twins/services/crm/models.py
from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from enterprise_twins.common.db.base import Base, ScenarioOwned, Timestamp


class Customer(ScenarioOwned, Base):
    __tablename__ = "crm_customers"
    __table_args__ = (UniqueConstraint("scenario_epoch", "customer_id"),)
    row_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    customer_id: Mapped[str] = mapped_column(String(80), index=True)
    display_name: Mapped[str] = mapped_column(String(200))
    primary_email: Mapped[str] = mapped_column(String(320), index=True)
    external_reference: Mapped[str] = mapped_column(String(120), index=True)
    account_status: Mapped[str] = mapped_column(String(40))
    contact_methods: Mapped[list[dict[str, Any]]] = mapped_column(JSONB)
    external_identifiers: Mapped[dict[str, str]] = mapped_column(JSONB)
    version: Mapped[int] = mapped_column(Integer, default=1)


class CustomerNote(ScenarioOwned, Base):
    __tablename__ = "crm_customer_notes"
    __table_args__ = (UniqueConstraint("scenario_epoch", "note_id"),)
    row_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    note_id: Mapped[str] = mapped_column(String(80), index=True)
    customer_id: Mapped[str] = mapped_column(String(80), index=True)
    body: Mapped[str] = mapped_column(Text)
    association: Mapped[str] = mapped_column(String(80))
    created_by: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(Timestamp)
    archived: Mapped[bool] = mapped_column(Boolean, default=False)
    version: Mapped[int] = mapped_column(Integer, default=1)
```

```python
# src/enterprise_twins/services/crm/settings.py
from pydantic_settings import BaseSettings, SettingsConfigDict


class CrmSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWINS_CRM_", extra="ignore")
    database_url: str
    cursor_secret: str = "crm-local-cursor-secret"
    identity_issuer: str = "http://identity:8000"
    identity_jwks_url: str = "http://identity:8000/.well-known/jwks.json"
    identity_audience: str = "enterprise-twins"
    control_url: str = "http://control:8000"
    control_token: str = "twin-local-token"
    relay_url: str = "http://event-relay-api:8000"
    relay_token: str = "crm-relay-local-token"
    participant_token: str = "participant-local-token"
```

- [ ] **Step 5: Add stable API schemas and cursor encoding**

```python
# selected definitions in src/enterprise_twins/services/crm/schemas.py
import base64
import hashlib
import hmac
import json
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from enterprise_twins.common.canonical import canonical_json


class CustomerView(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    customer_id: str = Field(alias="customerId")
    display_name: str = Field(alias="displayName")
    primary_email: str = Field(alias="primaryEmail")
    external_reference: str = Field(alias="externalReference")
    account_status: str = Field(alias="accountStatus")
    contact_methods: list[dict[str, object]] = Field(alias="contactMethods")
    external_identifiers: dict[str, str] = Field(alias="externalIdentifiers")
    version: int


class NoteCreate(BaseModel):
    body: str = Field(min_length=1, max_length=10_000)
    association: str = Field(min_length=1, max_length=80)


class NoteView(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    note_id: str = Field(alias="noteId")
    customer_id: str = Field(alias="customerId")
    body: str
    association: str
    created_by: str = Field(alias="createdBy")
    created_at: datetime = Field(alias="createdAt")
    archived: bool
    version: int


class CustomerPage(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    items: list[CustomerView]
    next_cursor: str | None = Field(default=None, alias="nextCursor")


class NotePage(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    items: list[NoteView]
    next_cursor: str | None = Field(default=None, alias="nextCursor")


def b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def b64decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def encode_cursor(customer_id: str, secret: str) -> str:
    payload = canonical_json({"customerId": customer_id})
    digest = hmac.new(secret.encode(), payload, hashlib.sha256).digest()
    return f"{b64encode(payload)}.{b64encode(digest)}"


def decode_cursor(value: str, secret: str) -> str:
    payload_part, supplied_part = value.split(".", 1)
    payload = b64decode(payload_part)
    supplied = b64decode(supplied_part)
    expected = hmac.new(secret.encode(), payload, hashlib.sha256).digest()
    if not hmac.compare_digest(supplied, expected):
        raise ValueError("cursor signature differs")
    customer_id = json.loads(payload)["customerId"]
    if not isinstance(customer_id, str):
        raise ValueError("cursor customer ID is invalid")
    return customer_id
```

- [ ] **Step 6: Implement exact repository searches**

Implement the repository with an active-epoch filter on every query:

```python
# src/enterprise_twins/services/crm/repository.py
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.db.records import ScenarioState
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.services.crm.models import Customer, CustomerNote
from enterprise_twins.services.crm.schemas import (
    CustomerPage,
    CustomerView,
    NotePage,
    NoteView,
    decode_cursor,
    encode_cursor,
)


def customer_view(item: Customer) -> CustomerView:
    return CustomerView(
        customerId=item.customer_id,
        displayName=item.display_name,
        primaryEmail=item.primary_email,
        externalReference=item.external_reference,
        accountStatus=item.account_status,
        contactMethods=item.contact_methods,
        externalIdentifiers=item.external_identifiers,
        version=item.version,
    )


def note_view(item: CustomerNote) -> NoteView:
    return NoteView(
        noteId=item.note_id,
        customerId=item.customer_id,
        body=item.body,
        association=item.association,
        createdBy=item.created_by,
        createdAt=item.created_at,
        archived=item.archived,
        version=item.version,
    )


class CustomerRepository:
    def __init__(self, factory: async_sessionmaker[AsyncSession], cursor_secret: str) -> None:
        self.factory = factory
        self.cursor_secret = cursor_secret

    async def active_epoch(self, session: AsyncSession) -> str:
        state = await session.get(ScenarioState, 1)
        if state is None or state.mode != "active":
            raise ApiError(
                ErrorCode.TEMPORARILY_UNAVAILABLE,
                "CRM scenario is not active",
                status_code=503,
                retryable=True,
            )
        return state.active_epoch

    async def search(
        self,
        *,
        email: str | None,
        external_reference: str | None,
        identifier: str | None,
        limit: int,
        after: str | None,
    ) -> CustomerPage:
        async with self.factory() as session:
            epoch = await self.active_epoch(session)
            statement = select(Customer).where(Customer.scenario_epoch == epoch)
            if email is not None:
                statement = statement.where(func.lower(Customer.primary_email) == email.casefold())
            if external_reference is not None:
                statement = statement.where(Customer.external_reference == external_reference)
            if identifier is not None:
                statement = statement.where(
                    (Customer.customer_id == identifier)
                    | Customer.external_identifiers.contains({"loyalty": identifier})
                )
            if after is not None:
                try:
                    boundary = decode_cursor(after, self.cursor_secret)
                except (ValueError, KeyError, TypeError) as error:
                    raise ApiError(ErrorCode.INVALID_REQUEST, "pagination cursor is invalid", status_code=422) from error
                statement = statement.where(Customer.customer_id > boundary)
            rows = list(
                await session.scalars(statement.order_by(Customer.customer_id).limit(limit + 1))
            )
            has_more = len(rows) > limit
            rows = rows[:limit]
            return CustomerPage(
                items=[customer_view(item) for item in rows],
                nextCursor=encode_cursor(rows[-1].customer_id, self.cursor_secret)
                if has_more and rows
                else None,
            )

    async def get(self, customer_id: str) -> CustomerView:
        async with self.factory() as session:
            epoch = await self.active_epoch(session)
            customer = await session.scalar(
                select(Customer).where(
                    Customer.scenario_epoch == epoch,
                    Customer.customer_id == customer_id,
                )
            )
            if customer is None:
                raise ApiError(ErrorCode.NOT_FOUND, "customer was not found", status_code=404)
            return customer_view(customer)

    async def list_notes(self, customer_id: str, include_archived: bool) -> NotePage:
        async with self.factory() as session:
            epoch = await self.active_epoch(session)
            customer_exists = await session.scalar(
                select(Customer.row_id).where(
                    Customer.scenario_epoch == epoch,
                    Customer.customer_id == customer_id,
                )
            )
            if customer_exists is None:
                raise ApiError(ErrorCode.NOT_FOUND, "customer was not found", status_code=404)
            statement = select(CustomerNote).where(
                CustomerNote.scenario_epoch == epoch,
                CustomerNote.customer_id == customer_id,
            )
            if not include_archived:
                statement = statement.where(CustomerNote.archived.is_(False))
            notes = await session.scalars(statement.order_by(CustomerNote.created_at, CustomerNote.note_id))
            return NotePage(items=[note_view(item) for item in notes])
```

Search fetches `limit + 1`, sorts only by customer ID, and never ranks or
collapses results.

- [ ] **Step 7: Implement idempotent note creation and post-commit faults**

```python
# src/enterprise_twins/services/crm/service.py
import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from enterprise_twins.common.auth.claims import Principal
from enterprise_twins.common.canonical import sha256_hex
from enterprise_twins.common.control.contracts import (
    FaultDecision,
    FaultEffect,
    FaultPhase,
    FaultProbe,
)
from enterprise_twins.common.db.idempotency import (
    IdempotencyNamespace,
    StoredResponse,
    run_idempotent,
)
from enterprise_twins.common.events.publisher import record_audit, record_event
from enterprise_twins.common.http.context import current_request
from enterprise_twins.common.http.errors import ApiError, ErrorCode
from enterprise_twins.common.ids import new_id
from enterprise_twins.services.crm.models import Customer, CustomerNote
from enterprise_twins.services.crm.repository import note_view
from enterprise_twins.services.crm.schemas import NoteCreate


class CrmControl(Protocol):
    async def now(self) -> datetime:
        raise NotImplementedError

    async def current_epoch(self) -> str:
        raise NotImplementedError

    async def evaluate_fault(self, probe: FaultProbe) -> FaultDecision:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class NoteWriteResult:
    response: StoredResponse
    replayed: bool
    fault: FaultDecision


class CrmService:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        control: CrmControl,
    ) -> None:
        self.factory = factory
        self.control = control

    async def create_note(
        self,
        customer_id: str,
        request: NoteCreate,
        expected_version: int,
        idempotency_key: str,
        principal: Principal,
    ) -> NoteWriteResult:
        context = current_request.get()
        if context is None:
            raise RuntimeError("request context is missing")
        now = await self.control.now()
        epoch = await self.control.current_epoch()
        namespace = IdempotencyNamespace(
            principal.tenant_id,
            principal.subject,
            "crm.note.create",
            idempotency_key,
        )
        payload = {
            "customerId": customer_id,
            "expectedVersion": expected_version,
            "body": request.model_dump(mode="json"),
        }
        async with self.factory.begin() as session:

            async def work() -> StoredResponse:
                customer = await session.scalar(
                    select(Customer)
                    .where(
                        Customer.scenario_epoch == epoch,
                        Customer.customer_id == customer_id,
                    )
                    .with_for_update()
                )
                if customer is None:
                    raise ApiError(ErrorCode.NOT_FOUND, "customer was not found", status_code=404)
                if customer.version != expected_version:
                    raise ApiError(
                        ErrorCode.CONFLICT,
                        "customer version differs",
                        status_code=409,
                        details={
                            "expectedVersion": expected_version,
                            "currentVersion": customer.version,
                        },
                    )
                note = CustomerNote(
                    row_id=new_id("nrow"),
                    note_id=new_id("note"),
                    scenario_epoch=epoch,
                    customer_id=customer_id,
                    body=request.body,
                    association=request.association,
                    created_by=principal.subject,
                    created_at=now,
                    archived=False,
                    version=1,
                )
                session.add(note)
                customer.version += 1
                record_audit(
                    session,
                    epoch=epoch,
                    action="crm.note.created",
                    resource_type="customer_note",
                    resource_id=note.note_id,
                    actor_id=principal.subject,
                    correlation_id=context.correlation_id,
                    occurred_at=now,
                    details={"customerId": customer_id},
                )
                record_event(
                    session,
                    epoch=epoch,
                    event_type="crm.note.created",
                    source="crm",
                    subject=f"note/{note.note_id}",
                    resource_version=note.version,
                    correlation_id=context.correlation_id,
                    causation_id=context.request_id,
                    occurred_at=now,
                    data={"noteId": note.note_id, "customerId": customer_id},
                )
                return StoredResponse(
                    201,
                    note_view(note).model_dump(mode="json", by_alias=True),
                    {"ETag": '"1"', "X-Customer-Version": str(customer.version)},
                )

            response, replayed = await run_idempotent(
                session,
                epoch,
                namespace,
                payload,
                work,
            )
        fault = await self.control.evaluate_fault(
            FaultProbe(
                targetService="crm",
                operation="crm.note.create",
                phase=FaultPhase.AFTER_COMMIT,
                actorId=principal.subject,
                resourceId=customer_id,
                correlationId=context.correlation_id,
                requestHash=sha256_hex(request.model_dump(mode="json")),
            )
        )
        return NoteWriteResult(response, replayed, fault)


async def apply_post_commit_fault(result: NoteWriteResult) -> None:
    if result.fault.effect == FaultEffect.TIMEOUT:
        await asyncio.sleep((result.fault.delay_ms or 250) / 1000)
    elif result.fault.effect == FaultEffect.CONNECTION_LOSS:
        raise ConnectionResetError("injected after-commit connection loss")
```

The API converts the stored response to JSON, sets
`Idempotency-Replayed: true|false`, and returns the stored headers. Route scope
requirements are `crm:read` for reads and `crm:notes:write` for note creation.
For `MALFORMED_RESPONSE`, it returns `Response(content=b"{", status_code=200,
media_type="application/json")` after the commit.

- [ ] **Step 8: Add routes, reset loader, and lifecycle**

Implement the business router:

```python
# core of src/enterprise_twins/services/crm/api.py
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query, Response
from fastapi.responses import JSONResponse


def crm_router(
    repository: CustomerRepository,
    service: CrmService,
    authenticator: BearerAuthenticator,
    relay: RelayClient | None,
) -> APIRouter:
    router = APIRouter()
    AnyPrincipal = Annotated[Principal, Depends(authenticator.authenticate)]
    ReadPrincipal = Annotated[Principal, Depends(require_scopes(authenticator, "crm:read"))]
    WritePrincipal = Annotated[Principal, Depends(require_scopes(authenticator, "crm:notes:write"))]
    WebhookPrincipal = Annotated[Principal, Depends(require_scopes(authenticator, "webhooks:manage"))]

    @router.get("/v1/customers")
    async def search_customers(
        _principal: ReadPrincipal,
        email: str | None = None,
        external_reference: str | None = Query(default=None, alias="externalReference"),
        identifier: str | None = None,
        limit: int = Query(default=50, ge=1, le=100),
        after: str | None = None,
    ) -> CustomerPage:
        return await repository.search(
            email=email,
            external_reference=external_reference,
            identifier=identifier,
            limit=limit,
            after=after,
        )

    @router.get("/v1/customers/{customer_id}")
    async def get_customer(customer_id: str, _principal: ReadPrincipal, response: Response) -> CustomerView:
        customer = await repository.get(customer_id)
        response.headers["ETag"] = f'"{customer.version}"'
        response.headers["X-Resource-Version"] = str(customer.version)
        return customer

    @router.get("/v1/customers/{customer_id}/notes")
    async def list_notes(
        customer_id: str,
        _principal: ReadPrincipal,
        include_archived: bool = Query(default=False, alias="includeArchived"),
    ) -> NotePage:
        return await repository.list_notes(customer_id, include_archived)

    @router.post("/v1/customers/{customer_id}/notes")
    async def create_note(
        customer_id: str,
        body: NoteCreate,
        principal: WritePrincipal,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
        if_match: Annotated[str, Header(alias="If-Match")],
    ) -> Response:
        try:
            expected_version = int(if_match.strip('"'))
        except ValueError as error:
            raise ApiError(ErrorCode.INVALID_REQUEST, "If-Match is invalid", status_code=422) from error
        result = await service.create_note(
            customer_id,
            body,
            expected_version,
            idempotency_key,
            principal,
        )
        await apply_post_commit_fault(result)
        if result.fault.effect == FaultEffect.MALFORMED_RESPONSE:
            return Response(content=b"{", status_code=200, media_type="application/json")
        headers = result.response.headers | {
            "Idempotency-Replayed": str(result.replayed).lower(),
        }
        return JSONResponse(
            result.response.body,
            status_code=result.response.status_code,
            headers=headers,
        )

    @router.get("/v1/capabilities")
    async def capabilities(_principal: AnyPrincipal) -> dict[str, object]:
        return {"service": "crm", "capabilities": ["crm:read", "crm:notes:write", "webhooks:manage"]}

    @router.post("/v1/webhook-subscriptions", status_code=201)
    async def create_subscription(
        body: WebhookSubscriptionCreate,
        principal: WebhookPrincipal,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> WebhookSubscriptionCreated:
        if relay is None:
            raise ApiError(ErrorCode.TEMPORARILY_UNAVAILABLE, "event relay is unavailable", status_code=503, retryable=True)
        return await relay.create_subscription(principal.subject, idempotency_key, body)

    @router.get("/v1/webhook-subscriptions")
    async def list_subscriptions(_principal: WebhookPrincipal) -> list[WebhookSubscriptionView]:
        if relay is None:
            raise ApiError(ErrorCode.TEMPORARILY_UNAVAILABLE, "event relay is unavailable", status_code=503, retryable=True)
        return await relay.list_subscriptions()

    @router.delete("/v1/webhook-subscriptions/{subscription_id}", status_code=204)
    async def delete_subscription(
        subscription_id: str,
        principal: WebhookPrincipal,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
        if_match: Annotated[str, Header(alias="If-Match")],
    ) -> None:
        if relay is None:
            raise ApiError(ErrorCode.TEMPORARILY_UNAVAILABLE, "event relay is unavailable", status_code=503, retryable=True)
        try:
            version = int(if_match.strip('"'))
        except ValueError as error:
            raise ApiError(ErrorCode.INVALID_REQUEST, "If-Match is invalid", status_code=422) from error
        await relay.delete_subscription(principal.subject, idempotency_key, subscription_id, version)

    return router
```

Implement the reset loader:

```python
# src/enterprise_twins/services/crm/scenario.py
from datetime import datetime
from typing import Any

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from enterprise_twins.common.ids import new_id
from enterprise_twins.services.crm.models import Customer, CustomerNote


class CrmScenarioLoader:
    async def load(
        self,
        session: AsyncSession,
        epoch: str,
        payload: dict[str, Any],
    ) -> dict[str, object]:
        customers = payload["customers"]
        notes = payload["notes"]
        customer_ids = [item["customerId"] for item in customers]
        if len(customer_ids) != len(set(customer_ids)):
            raise ValueError("CRM customer IDs must be unique")
        known = set(customer_ids)
        for item in customers:
            session.add(
                Customer(
                    row_id=new_id("crow"),
                    scenario_epoch=epoch,
                    customer_id=item["customerId"],
                    display_name=item["displayName"],
                    primary_email=item["primaryEmail"].casefold(),
                    external_reference=item["externalReference"],
                    account_status=item["accountStatus"],
                    contact_methods=item["contactMethods"],
                    external_identifiers=item["externalIdentifiers"],
                    version=item["version"],
                )
            )
        for item in notes:
            if item["customerId"] not in known:
                raise ValueError("CRM note refers to an unknown customer")
            session.add(
                CustomerNote(
                    row_id=new_id("nrow"),
                    note_id=item["noteId"],
                    scenario_epoch=epoch,
                    customer_id=item["customerId"],
                    body=item["body"],
                    association=item["association"],
                    created_by=item["createdBy"],
                    created_at=datetime.fromisoformat(item["createdAt"].replace("Z", "+00:00")),
                    archived=item.get("archived", False),
                    version=item.get("version", 1),
                )
            )
        return {
            "schemaVersion": payload["schemaVersion"],
            "counts": {"customers": len(customers), "notes": len(notes)},
            "aliases": payload.get("aliases", {}),
        }

    async def discard(self, session: AsyncSession, epoch: str) -> None:
        await session.execute(delete(CustomerNote).where(CustomerNote.scenario_epoch == epoch))
        await session.execute(delete(Customer).where(Customer.scenario_epoch == epoch))
```

Create the testable app factory:

```python
# core of src/enterprise_twins/services/crm/app.py
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager

from fastapi import FastAPI
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class CrmStatus:
    def __init__(self, factory: async_sessionmaker[AsyncSession]) -> None:
        self.factory = factory

    async def state(self) -> ScenarioState:
        async with self.factory() as session:
            state = await session.get(ScenarioState, 1)
            if state is None:
                raise RuntimeError("CRM scenario is not initialised")
            return state

    async def current_epoch(self) -> str:
        return (await self.state()).active_epoch

    async def readiness(self) -> tuple[bool, dict[str, str]]:
        try:
            state = await self.state()
        except RuntimeError:
            return False, {"database": "not_ready", "scenario": "uninitialised"}
        return state.mode == "active", {"database": "ready", "scenario": state.mode}


def create_crm_app(
    factory: async_sessionmaker[AsyncSession],
    settings: CrmSettings,
    control: CrmControl,
    verifier: JwtVerifier,
    relay: RelayClient | None = None,
    lifespan: Callable[[FastAPI], AbstractAsyncContextManager[None]] | None = None,
) -> FastAPI:
    authenticator = BearerAuthenticator(
        verifier,
        DatabaseAuthDecisionRecorder("crm", factory, control),
    )
    repository = CustomerRepository(factory, settings.cursor_secret)
    service = CrmService(factory, control)
    return create_app(
        "CRM twin",
        ("crm:read", "crm:notes:write", "webhooks:manage"),
        CrmStatus(factory),
        (crm_router(repository, service, authenticator, relay),),
        lifespan,
    )
```

Add this environment factory to the same file:

```python
def create_from_env() -> FastAPI:
    settings = CrmSettings()
    engine = make_engine(settings.database_url)
    factory = make_session_factory(engine)
    http_client = httpx.AsyncClient()
    control = ControlClient(settings.control_url, settings.control_token, http_client)
    verifier = JwtVerifier(
        settings.identity_issuer,
        settings.identity_audience,
        settings.identity_jwks_url,
        control,
        http_client,
    )
    relay = RelayClient(settings.relay_url, "crm", settings.relay_token, http_client)
    dispatcher = OutboxDispatcher(factory, relay)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        async def dispatch() -> None:
            while True:
                try:
                    await dispatcher.run_once()
                except SQLAlchemyError:
                    pass
                await asyncio.sleep(0.05)

        task = asyncio.create_task(dispatch())
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            await http_client.aclose()
            await engine.dispose()

    return create_crm_app(factory, settings, control, verifier, relay, lifespan)
```

```python
# src/enterprise_twins/services/crm/admin_app.py
from fastapi import FastAPI


def create_from_env() -> FastAPI:
    settings = CrmSettings()
    factory = make_session_factory(make_engine(settings.database_url))
    participant = ResetParticipant(factory, CrmScenarioLoader(), "crm")
    return create_participant_app("CRM", participant, settings.participant_token)
```

Import the reset participant, database factory, settings, and loader in the
admin module. The public module imports `asyncio`, `httpx`,
`asynccontextmanager`, `suppress`, and the common clients shown in the factory.

- [ ] **Step 9: Run CRM tests and quality checks**

Run:

```bash
docker compose build test-runner
docker compose run --rm test-runner pytest tests/contract/crm -q
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy
```

Expected: search returns 1, 2, and 0 items as asserted; note replay creates one note; denial cases pass; static checks report no errors.

- [ ] **Step 10: Commit**

```bash
git add src/enterprise_twins/services/crm tests/contract/crm
git commit -m "feat: add stateful crm twin"
```

### Task 10: Service migrations, deterministic seed, and Compose topology

**Files:**
- Create: `alembic.ini`
- Create: `src/enterprise_twins/migration_metadata.py`
- Create: `src/enterprise_twins/migration_runner.py`
- Create: `src/enterprise_twins/alembic/__init__.py`
- Create: `src/enterprise_twins/alembic/env.py`
- Create: `src/enterprise_twins/alembic/versions/__init__.py`
- Create: `src/enterprise_twins/alembic/versions/0001_platform_contracts.py`
- Create: `src/enterprise_twins/runtime.py`
- Create: `scenarios/base/platform-contracts/manifest.json`
- Create: `scenarios/base/platform-contracts/identity.json`
- Create: `scenarios/base/platform-contracts/crm.json`
- Create: `scenarios/base/platform-contracts/relay.json`
- Create: `scripts/update-scenario-checksums`
- Modify: `Dockerfile`
- Modify: `compose.yaml`
- Create: `tests/integration/test_topology.py`
- Create: `tests/integration/test_reset.py`

**Interfaces:**
- Consumes: all service factories and reset loaders from Tasks 4 through 9.
- Produces: idempotent per-service schema upgrades; complete platform-contracts scenario; running `control`, `event-relay-api`, `event-relay-worker`, `event-relay-admin`, `identity`, `identity-admin`, `crm`, and `crm-admin` services; host business ports 8101 and 8102 only.

- [ ] **Step 1: Write topology and reset integration tests**

```python
# tests/integration/test_topology.py
import subprocess

import httpx


def test_only_business_apis_are_published() -> None:
    assert httpx.get("http://127.0.0.1:8101/health/live").status_code == 200
    assert httpx.get("http://127.0.0.1:8102/health/live").status_code == 200
    for service, port in (
        ("postgres", "5432"),
        ("control", "8000"),
        ("event-relay-api", "8000"),
        ("identity-admin", "9000"),
        ("crm-admin", "9000"),
    ):
        result = subprocess.run(
            ["docker", "compose", "port", service, port],
            check=False,
            capture_output=True,
            text=True,
        )
        assert result.stdout.strip() == ""
```

```python
# tests/integration/test_reset.py
import os

import httpx
import pytest


@pytest.mark.asyncio
async def test_same_seed_has_same_checksum() -> None:
    control = os.environ["CONTROL_URL"]
    headers = {"Authorization": f"Bearer {os.environ['CONTROL_TOKEN']}"}
    async with httpx.AsyncClient() as client:
        first = await client.post(
            f"{control}/control/v1/reset",
            headers=headers,
            json={"scenarioId": "platform-contracts", "version": 1, "randomSeed": 7},
        )
        second = await client.post(
            f"{control}/control/v1/reset",
            headers=headers,
            json={"scenarioId": "platform-contracts", "version": 1, "randomSeed": 7},
        )
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["manifestChecksum"] == second.json()["manifestChecksum"]
    assert first.json()["scenarioEpoch"] != second.json()["scenarioEpoch"]
    assert {report["service"] for report in second.json()["reports"]} == {"identity", "crm", "relay"}
```

Run the reset test inside `test-runner`, where `CONTROL_URL=http://control:8000`.

- [ ] **Step 2: Run integration tests to verify they fail**

Run: `uv run pytest tests/integration/test_topology.py -q`  
Expected: FAIL because ports 8101 and 8102 have no services.

- [ ] **Step 3: Add a service-filtered Alembic environment**

```ini
# alembic.ini
[alembic]
script_location = src/enterprise_twins/alembic
prepend_sys_path = .
```

```python
# src/enterprise_twins/migration_metadata.py
from sqlalchemy import MetaData

from enterprise_twins.common.db.base import Base
from enterprise_twins.services.control import models as control_models
from enterprise_twins.services.crm import models as crm_models
from enterprise_twins.services.identity import models as identity_models
from enterprise_twins.services.relay import models as relay_models

REGISTERED_MODELS = (control_models, crm_models, identity_models, relay_models)

SERVICE_TABLES = {
    "control": {"scenario_state", "virtual_clock", "fault_rules", "fault_activations", "reset_runs"},
    "relay": {"scenario_state", "idempotency_records", "relay_subscriptions", "relay_source_events", "relay_deliveries", "relay_delivery_attempts"},
    "identity": {"scenario_state", "audit_records", "idempotency_records", "outbox_records", "identity_clients"},
    "crm": {"scenario_state", "audit_records", "idempotency_records", "outbox_records", "crm_customers", "crm_customer_notes"},
}


def selected_metadata(service: str) -> MetaData:
    if service not in SERVICE_TABLES:
        raise ValueError(f"unknown migration service: {service}")
    metadata = MetaData()
    for name in sorted(SERVICE_TABLES[service]):
        Base.metadata.tables[name].to_metadata(metadata)
    return metadata
```

```python
# src/enterprise_twins/alembic/env.py
import asyncio
import os

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from enterprise_twins.migration_metadata import selected_metadata


def run_sync_migrations(connection: Connection, service: str) -> None:
    context.configure(
        connection=connection,
        target_metadata=selected_metadata(service),
        version_table=f"alembic_version_{service}",
    )
    with context.begin_transaction():
        context.run_migrations()


async def run() -> None:
    service = os.environ["TWINS_MIGRATION_SERVICE"]
    configuration = context.config.get_section(context.config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = os.environ["TWINS_DATABASE_URL"]
    engine = async_engine_from_config(configuration, prefix="sqlalchemy.")
    async with engine.connect() as connection:
        await connection.run_sync(run_sync_migrations, service)
    await engine.dispose()


asyncio.run(run())
```

```python
# src/enterprise_twins/alembic/versions/0001_platform_contracts.py
from alembic import context, op

from enterprise_twins.migration_metadata import selected_metadata

revision = "0001_platform_contracts"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    service = context.get_x_argument(as_dictionary=True)["service"]
    selected_metadata(service).create_all(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    service = context.get_x_argument(as_dictionary=True)["service"]
    selected_metadata(service).drop_all(bind=op.get_bind(), checkfirst=True)
```

```python
# src/enterprise_twins/migration_runner.py
import argparse
import os
from argparse import Namespace

from alembic import command
from alembic.config import Config


def upgrade(service: str, database_url: str) -> None:
    os.environ["TWINS_MIGRATION_SERVICE"] = service
    os.environ["TWINS_DATABASE_URL"] = database_url
    configuration = Config("alembic.ini")
    configuration.cmd_opts = Namespace(x=[f"service={service}"])
    command.upgrade(configuration, "head")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("service", choices=["control", "relay", "identity", "crm"])
    args = parser.parse_args()
    upgrade(args.service, os.environ["TWINS_DATABASE_URL"])


if __name__ == "__main__":
    main()
```

Run the same upgrade twice in its test and assert the second run succeeds.

- [ ] **Step 4: Add a process launcher that migrates before binding**

```python
# src/enterprise_twins/runtime.py
import argparse
import os

import uvicorn

from enterprise_twins.migration_runner import upgrade


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("service", choices=["control", "relay", "identity", "crm"])
    parser.add_argument("application")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    upgrade(args.service, os.environ["TWINS_DATABASE_URL"])
    uvicorn.run(args.application, host="0.0.0.0", port=args.port, factory=True)


if __name__ == "__main__":
    main()
```

Admin and public factories both run the same idempotent migration. PostgreSQL's
Alembic version table serialises concurrent first startup.
Set the Docker build-stage source-copy lines to:

```dockerfile
COPY alembic.ini ./alembic.ini
COPY src ./src
RUN uv sync --locked --no-dev
```

- [ ] **Step 5: Add the exact platform scenario payloads**

`scenarios/base/platform-contracts/identity.json`:

```json
{
  "schemaVersion": "1",
  "expectedCounts": {"clients": 3},
  "aliases": {
    "supportAgent": "support-agent",
    "readOnlyEvaluator": "read-only-evaluator",
    "webhookManager": "webhook-manager"
  },
  "clients": [
    {
      "clientId": "support-agent",
      "clientSecret": "support-secret",
      "subject": "person-support-1",
      "actorType": "human",
      "role": "support_agent",
      "scopes": ["crm:read", "crm:notes:write"],
      "tenantId": "tenant_synthetic"
    },
    {
      "clientId": "read-only-evaluator",
      "clientSecret": "evaluator-secret",
      "subject": "service-evaluator-1",
      "actorType": "service",
      "role": "evaluator_service",
      "scopes": ["crm:read"],
      "tenantId": "tenant_synthetic"
    },
    {
      "clientId": "webhook-manager",
      "clientSecret": "webhook-secret",
      "subject": "service-webhook-manager-1",
      "actorType": "service",
      "role": "platform_observer",
      "scopes": ["webhooks:manage"],
      "tenantId": "tenant_synthetic"
    }
  ]
}
```

`scenarios/base/platform-contracts/crm.json`:

```json
{
  "schemaVersion": "1",
  "expectedCounts": {"customers": 3, "notes": 0},
  "aliases": {"uniqueCustomer": "cus_unique"},
  "customers": [
    {
      "customerId": "cus_unique",
      "displayName": "Alex Unique",
      "primaryEmail": "alex.unique@example.test",
      "externalReference": "ext-unique",
      "accountStatus": "active",
      "contactMethods": [{"type": "email", "value": "alex.unique@example.test", "primary": true}],
      "externalIdentifiers": {"loyalty": "LOY-1001"},
      "version": 1
    },
    {
      "customerId": "cus_ambiguous_a",
      "displayName": "Sam Shared A",
      "primaryEmail": "shared@example.test",
      "externalReference": "ext-shared-a",
      "accountStatus": "active",
      "contactMethods": [{"type": "email", "value": "shared@example.test", "primary": true}],
      "externalIdentifiers": {"loyalty": "LOY-2001"},
      "version": 1
    },
    {
      "customerId": "cus_ambiguous_b",
      "displayName": "Sam Shared B",
      "primaryEmail": "shared@example.test",
      "externalReference": "ext-shared-b",
      "accountStatus": "active",
      "contactMethods": [{"type": "email", "value": "shared@example.test", "primary": true}],
      "externalIdentifiers": {"loyalty": "LOY-2002"},
      "version": 1
    }
  ],
  "notes": []
}
```

`scenarios/base/platform-contracts/relay.json`:

```json
{
  "schemaVersion": "1",
  "expectedCounts": {"subscriptions": 0, "events": 0, "deliveries": 0, "attempts": 0},
  "aliases": {},
  "subscriptions": []
}
```

Create the manifest with this script, then run it once and commit its output:

```python
#!/usr/bin/env python3
# scripts/update-scenario-checksums
import hashlib
import json
from pathlib import Path

ROOT = Path("scenarios/base/platform-contracts")
FILES = {"identity": "identity.json", "crm": "crm.json", "relay": "relay.json"}


def digest(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(encoded).hexdigest()


services: dict[str, dict[str, str]] = {}
for service, filename in FILES.items():
    payload = json.loads((ROOT / filename).read_text(encoding="utf-8"))
    services[service] = {"file": filename, "checksum": digest(payload)}

manifest = {
    "scenarioId": "platform-contracts",
    "version": 1,
    "initialTime": "2026-08-19T10:00:00Z",
    "services": services,
}
(ROOT / "manifest.json").write_text(
    json.dumps(manifest, sort_keys=True, indent=2) + "\n",
    encoding="utf-8",
)
```

- [ ] **Step 6: Complete Compose with strict process and network membership**

Use one YAML service extension for image, environment, and PostgreSQL startup.
The final membership and host publication must equal this table:

| Service | Process | Networks | Host port |
|---|---|---|---:|
| control | Control API, CLI, bootstrap-reset task | twin-control | none |
| event-relay-api | integration API | twin-integration, twin-control | none |
| event-relay-worker | delivery loop, no listener | twin-integration, twin-webhook-egress, twin-control | none |
| event-relay-admin | reset participant | twin-control | none |
| identity | public API and outbox loop | twin-public, twin-integration, twin-control | 127.0.0.1:8101 |
| identity-admin | reset participant | twin-control | none |
| crm | public API and outbox loop | twin-public, twin-integration, twin-control | 127.0.0.1:8102 |
| crm-admin | reset participant | twin-control | none |
| postgres | database listener | twin-integration, twin-control | none |
| test-runner | pytest process under test profile | twin-public, twin-control | none |

Use database URLs with their matching users from Task 1. Use synthetic private
tokens `controller-local-token`, `twin-local-token`,
`participant-local-token`, `identity-relay-local-token`, and
`crm-relay-local-token`. Configure Relay's source-token map and target allowlist
as JSON environment values. The Control participant map points at the three
admin services. Public app healthchecks use `/health/ready`. No service mounts
the Docker socket.

Add these services to the Task 1 Compose file. Retain its PostgreSQL service,
volume, four networks, and test runner:

```yaml
  identity-admin:
    build: .
    command:
      - enterprise_twins.runtime
      - identity
      - enterprise_twins.services.identity.admin_app:create_from_env
      - --port
      - "9000"
    environment:
      TWINS_DATABASE_URL: postgresql+asyncpg://identity_user:identity_local_only@postgres/identity
      TWINS_IDENTITY_DATABASE_URL: postgresql+asyncpg://identity_user:identity_local_only@postgres/identity
      TWINS_IDENTITY_PARTICIPANT_TOKEN: participant-local-token
      TWINS_IDENTITY_SECRET_PEPPER: identity-local-pepper
    depends_on:
      postgres: {condition: service_healthy}
    healthcheck: &admin-health
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9000/health/ready', timeout=1)"]
      interval: 2s
      timeout: 2s
      retries: 30
    networks: [twin-control]

  crm-admin:
    build: .
    command:
      - enterprise_twins.runtime
      - crm
      - enterprise_twins.services.crm.admin_app:create_from_env
      - --port
      - "9000"
    environment:
      TWINS_DATABASE_URL: postgresql+asyncpg://crm_user:crm_local_only@postgres/crm
      TWINS_CRM_DATABASE_URL: postgresql+asyncpg://crm_user:crm_local_only@postgres/crm
      TWINS_CRM_PARTICIPANT_TOKEN: participant-local-token
    depends_on:
      postgres: {condition: service_healthy}
    healthcheck: *admin-health
    networks: [twin-control]

  event-relay-admin:
    build: .
    command:
      - enterprise_twins.runtime
      - relay
      - enterprise_twins.services.relay.admin_app:create_from_env
      - --port
      - "9000"
    environment:
      TWINS_DATABASE_URL: postgresql+asyncpg://relay_user:relay_local_only@postgres/relay
      TWINS_RELAY_DATABASE_URL: postgresql+asyncpg://relay_user:relay_local_only@postgres/relay
      TWINS_RELAY_PARTICIPANT_TOKEN: participant-local-token
    depends_on:
      postgres: {condition: service_healthy}
    healthcheck: *admin-health
    networks: [twin-control]

  control:
    build: .
    command:
      - enterprise_twins.runtime
      - control
      - enterprise_twins.services.control.app:create_from_env
    environment:
      TWINS_DATABASE_URL: postgresql+asyncpg://control_user:control_local_only@postgres/control
      TWINS_CONTROL_DATABASE_URL: postgresql+asyncpg://control_user:control_local_only@postgres/control
      TWINS_CONTROL_CONTROLLER_TOKEN: controller-local-token
      TWINS_CONTROL_TWIN_TOKEN: twin-local-token
      TWINS_CONTROL_PARTICIPANT_TOKEN: participant-local-token
      TWINS_CONTROL_PARTICIPANTS: >-
        {"identity":"http://identity-admin:9000","crm":"http://crm-admin:9000","relay":"http://event-relay-admin:9000"}
      TWINS_CONTROL_SCENARIO_ROOT: /app/scenarios/base
      TWINS_CONTROL_BOOTSTRAP_SCENARIO: platform-contracts
      TWINS_CONTROL_BOOTSTRAP_VERSION: "1"
    volumes:
      - ./scenarios:/app/scenarios:ro
    depends_on:
      postgres: {condition: service_healthy}
      identity-admin: {condition: service_healthy}
      crm-admin: {condition: service_healthy}
      event-relay-admin: {condition: service_healthy}
    healthcheck: &public-health
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/ready', timeout=1)"]
      interval: 2s
      timeout: 2s
      retries: 45
    networks: [twin-control]

  event-relay-api:
    build: .
    command:
      - enterprise_twins.runtime
      - relay
      - enterprise_twins.services.relay.app:create_from_env
    environment: &relay-environment
      TWINS_DATABASE_URL: postgresql+asyncpg://relay_user:relay_local_only@postgres/relay
      TWINS_RELAY_DATABASE_URL: postgresql+asyncpg://relay_user:relay_local_only@postgres/relay
      TWINS_RELAY_CONTROL_URL: http://control:8000
      TWINS_RELAY_CONTROL_TOKEN: twin-local-token
      TWINS_RELAY_SOURCE_TOKENS: >-
        {"identity":"identity-relay-local-token","crm":"crm-relay-local-token"}
      TWINS_RELAY_ALLOWED_TARGETS: '["webhook-receiver","host.docker.internal"]'
    depends_on:
      event-relay-admin: {condition: service_healthy}
      control: {condition: service_started}
    healthcheck: *public-health
    networks: [twin-integration, twin-control]

  event-relay-worker:
    build: .
    command: [enterprise_twins.services.relay.delivery]
    environment: *relay-environment
    depends_on:
      event-relay-api: {condition: service_healthy}
    networks: [twin-integration, twin-webhook-egress, twin-control]

  identity:
    build: .
    command:
      - enterprise_twins.runtime
      - identity
      - enterprise_twins.services.identity.app:create_from_env
    environment:
      TWINS_DATABASE_URL: postgresql+asyncpg://identity_user:identity_local_only@postgres/identity
      TWINS_IDENTITY_DATABASE_URL: postgresql+asyncpg://identity_user:identity_local_only@postgres/identity
      TWINS_IDENTITY_ISSUER: http://identity:8000
      TWINS_IDENTITY_AUDIENCE: enterprise-twins
      TWINS_IDENTITY_SIGNING_SEED: identity-test-signing-seed
      TWINS_IDENTITY_SECRET_PEPPER: identity-local-pepper
      TWINS_IDENTITY_TOKEN_TTL_SECONDS: "600"
      TWINS_IDENTITY_CONTROL_URL: http://control:8000
      TWINS_IDENTITY_CONTROL_TOKEN: twin-local-token
      TWINS_IDENTITY_RELAY_URL: http://event-relay-api:8000
      TWINS_IDENTITY_RELAY_TOKEN: identity-relay-local-token
    depends_on:
      identity-admin: {condition: service_healthy}
      control: {condition: service_started}
      event-relay-api: {condition: service_started}
    healthcheck: *public-health
    ports: ["127.0.0.1:8101:8000"]
    networks: [twin-public, twin-integration, twin-control]

  crm:
    build: .
    command:
      - enterprise_twins.runtime
      - crm
      - enterprise_twins.services.crm.app:create_from_env
    environment:
      TWINS_DATABASE_URL: postgresql+asyncpg://crm_user:crm_local_only@postgres/crm
      TWINS_CRM_DATABASE_URL: postgresql+asyncpg://crm_user:crm_local_only@postgres/crm
      TWINS_CRM_CURSOR_SECRET: crm-local-cursor-secret
      TWINS_CRM_IDENTITY_ISSUER: http://identity:8000
      TWINS_CRM_IDENTITY_JWKS_URL: http://identity:8000/.well-known/jwks.json
      TWINS_CRM_IDENTITY_AUDIENCE: enterprise-twins
      TWINS_CRM_CONTROL_URL: http://control:8000
      TWINS_CRM_CONTROL_TOKEN: twin-local-token
      TWINS_CRM_RELAY_URL: http://event-relay-api:8000
      TWINS_CRM_RELAY_TOKEN: crm-relay-local-token
    depends_on:
      crm-admin: {condition: service_healthy}
      control: {condition: service_started}
      identity: {condition: service_started}
      event-relay-api: {condition: service_started}
    healthcheck: *public-health
    ports: ["127.0.0.1:8102:8000"]
    networks: [twin-public, twin-integration, twin-control]
```

Each `app.py` and `admin_app.py` exposes `create_from_env() -> FastAPI` and
constructs its settings and session factory from the environment shown above.
The Relay worker module exposes an async loop and calls it from
`if __name__ == "__main__"` without creating a listening socket.

Control starts a background bootstrap reset only when `ScenarioState` is
absent. It retries participant connection failures for up to 30 seconds, then
loads `platform-contracts` version 1 with derived seed. This allows
`docker compose up -d --build --wait` to finish with every business app on the
same active epoch.

- [ ] **Step 7: Complete the note-clearing assertion and run integration tests**

Add this second test to `test_reset.py`:

```python
async def support_token(client: httpx.AsyncClient) -> str:
    response = await client.post(
        "http://identity:8000/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "support-agent",
            "client_secret": "support-secret",
            "scope": "crm:read crm:notes:write",
        },
    )
    response.raise_for_status()
    return str(response.json()["access_token"])


@pytest.mark.asyncio
async def test_reset_clears_created_note() -> None:
    control_headers = {"Authorization": f"Bearer {os.environ['CONTROL_TOKEN']}"}
    reset_body = {"scenarioId": "platform-contracts", "version": 1, "randomSeed": 7}
    async with httpx.AsyncClient() as client:
        first_reset = await client.post(
            f"{os.environ['CONTROL_URL']}/control/v1/reset",
            headers=control_headers,
            json=reset_body,
        )
        first_reset.raise_for_status()
        token = await support_token(client)
        business_headers = {
            "Authorization": f"Bearer {token}",
            "X-Correlation-Id": "case-reset-test",
        }
        customer = await client.get("http://crm:8000/v1/customers/cus_unique", headers=business_headers)
        customer.raise_for_status()
        created = await client.post(
            "http://crm:8000/v1/customers/cus_unique/notes",
            headers=business_headers
            | {"Idempotency-Key": "reset-note", "If-Match": customer.headers["ETag"]},
            json={"body": "removed by reset", "association": "account"},
        )
        assert created.status_code == 201
        before = await client.get(
            "http://crm:8000/v1/customers/cus_unique/notes",
            headers=business_headers,
        )
        assert len(before.json()["items"]) == 1
        second_reset = await client.post(
            f"{os.environ['CONTROL_URL']}/control/v1/reset",
            headers=control_headers,
            json=reset_body,
        )
        second_reset.raise_for_status()
        new_token = await support_token(client)
        after = await client.get(
            "http://crm:8000/v1/customers/cus_unique/notes",
            headers={
                "Authorization": f"Bearer {new_token}",
                "X-Correlation-Id": "case-reset-test-after",
            },
        )
        assert after.json()["items"] == []
```

Run:

```bash
./scripts/update-scenario-checksums
docker compose up -d --build --wait
uv run pytest tests/integration/test_topology.py -q
docker compose run --rm test-runner pytest tests/integration/test_reset.py -q
docker compose exec -T control twins status
```

Expected: Compose reaches healthy state, host topology test passes, reset test proves the note is cleared, and status reports `platform-contracts` version 1 in active mode.

- [ ] **Step 8: Prove migrations and restart persistence**

Run:

```bash
docker compose exec -T identity python -m enterprise_twins.migration_runner identity
docker compose exec -T identity python -m enterprise_twins.migration_runner identity
docker compose restart identity
docker compose up -d --wait identity
curl --fail http://127.0.0.1:8101/health/ready
```

Expected: both migration commands exit 0 and Identity returns ready after restart without a reset.

- [ ] **Step 9: Run quality checks and commit**

Run:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy
docker compose config --quiet
git add alembic.ini Dockerfile src/enterprise_twins/migration_metadata.py src/enterprise_twins/migration_runner.py src/enterprise_twins/alembic src/enterprise_twins/runtime.py scenarios scripts/update-scenario-checksums compose.yaml tests/integration
git commit -m "feat: compose the platform proving estate"
```

Expected: all checks pass and the scenario manifest contains three non-empty 64-character checksums.

### Task 11: Black-box platform conformance and operator documentation

**Files:**
- Create: `src/enterprise_twins/conformance/receiver.py`
- Create: `src/enterprise_twins/conformance/platform_contracts.py`
- Create: `scripts/conformance`
- Modify: `compose.yaml`
- Create: `tests/conformance/test_platform_script.py`
- Create: `docs/development.md`

**Interfaces:**
- Consumes: only public Identity and CRM URLs plus private Control and receiver URLs. It consumes no database URL or Docker socket.
- Produces: `./scripts/conformance platform-contracts`; successful-call transcript, failure transcript, webhook transcript, reset checksums, fault activations, and restart proof under `artifacts/platform-contracts/`.

- [ ] **Step 1: Write a wrapper-level acceptance test**

```python
# tests/conformance/test_platform_script.py
import json
import subprocess
from pathlib import Path


def test_platform_contract_script_exports_success_and_failure_evidence() -> None:
    result = subprocess.run(
        ["./scripts/conformance", "platform-contracts"],
        check=False,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    root = Path("artifacts/platform-contracts")
    summary = json.loads((root / "summary.json").read_text())
    assert summary["status"] == "passed"
    assert summary["successfulSequence"] == "passed"
    assert summary["failureSequence"] == "passed"
    assert summary["restartPersistence"] == "passed"
    assert summary["webhookSignatures"] == "passed"
    assert len(json.loads((root / "fault-activations.json").read_text())) == 1
```

- [ ] **Step 2: Run the acceptance test to verify it fails**

Run: `uv run pytest tests/conformance/test_platform_script.py -q`  
Expected: FAIL because `scripts/conformance` does not exist.

- [ ] **Step 3: Implement the isolated webhook receiver**

```python
# src/enterprise_twins/conformance/receiver.py
import hashlib
import hmac
import os
from dataclasses import dataclass, field

from fastapi import APIRouter, Depends, FastAPI, Header, Request

from enterprise_twins.common.control.auth import require_token


@dataclass
class ReceiverState:
    secrets: list[str] = field(default_factory=list)
    events: list[dict[str, object]] = field(default_factory=list)


state = ReceiverState()
app = FastAPI(title="Twin conformance webhook receiver")
internal = APIRouter(
    prefix="/internal/v1",
    dependencies=[Depends(require_token(os.environ["TWINS_RECEIVER_CONTROL_TOKEN"]))],
)


@internal.post("/reset", status_code=204)
async def reset() -> None:
    state.secrets.clear()
    state.events.clear()


@internal.post("/secrets", status_code=204)
async def add_secret(body: dict[str, str]) -> None:
    state.secrets.append(body["secret"])


@internal.get("/events")
async def events() -> list[dict[str, object]]:
    return state.events


@app.post("/events", status_code=204)
async def receive(
    request: Request,
    x_twin_event_id: str = Header(alias="X-Twin-Event-Id"),
    x_twin_timestamp: str = Header(alias="X-Twin-Timestamp"),
    x_twin_signature: str = Header(alias="X-Twin-Signature"),
) -> None:
    body = await request.body()
    valid = any(
        hmac.compare_digest(
            x_twin_signature,
            "v1="
            + hmac.new(
                secret.encode(),
                x_twin_timestamp.encode() + b"." + body,
                hashlib.sha256,
            ).hexdigest(),
        )
        for secret in state.secrets
    )
    if not valid:
        raise ValueError("webhook signature differs")
    state.events.append(
        {
            "eventId": x_twin_event_id,
            "timestamp": x_twin_timestamp,
            "signatureValid": True,
            "body": body.decode(),
        }
    )


app.include_router(internal)
```

The
receiver process joins `twin-control` and `twin-webhook-egress`; the driver
joins `twin-control` and `twin-public` only.

- [ ] **Step 4: Implement the successful manual API sequence**

Create the driver module with this common state and successful phase:

```python
# src/enterprise_twins/conformance/platform_contracts.py
import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any

import httpx


async def require_status(response: httpx.Response, expected: int) -> Any:
    if response.status_code != expected:
        raise AssertionError(
            f"{response.request.method} {response.request.url} returned "
            f"{response.status_code}: {response.text}"
        )
    if not response.content:
        return {}
    return response.json()


class Driver:
    def __init__(self) -> None:
        self.identity = os.environ["IDENTITY_URL"]
        self.crm = os.environ["CRM_URL"]
        self.control = os.environ["CONTROL_URL"]
        self.receiver = os.environ["RECEIVER_URL"]
        self.control_headers = {
            "Authorization": f"Bearer {os.environ['CONTROL_TOKEN']}",
        }
        self.receiver_headers = {
            "Authorization": f"Bearer {os.environ['RECEIVER_TOKEN']}",
        }
        self.artifacts = Path(os.environ["ARTIFACT_ROOT"])
        self.artifacts.mkdir(parents=True, exist_ok=True)
        self.client = httpx.AsyncClient()
        self.success_calls: list[dict[str, object]] = []
        self.failure_calls: list[dict[str, object]] = []

    async def close(self) -> None:
        await self.client.aclose()

    def write(self, name: str, value: object) -> None:
        (self.artifacts / name).write_text(
            json.dumps(value, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )

    def record(self, target: list[dict[str, object]], operation: str, response: httpx.Response) -> None:
        target.append(
            {
                "operation": operation,
                "status": response.status_code,
                "requestId": response.headers.get("X-Request-Id"),
                "scenarioEpoch": response.headers.get("X-Scenario-Epoch"),
            }
        )

    async def reset(self) -> dict[str, object]:
        response = await self.client.post(
            f"{self.control}/control/v1/reset",
            headers=self.control_headers,
            json={"scenarioId": "platform-contracts", "version": 1, "randomSeed": 7},
        )
        return await require_status(response, 200)

    async def token(self, client_id: str, secret: str, scope: str) -> str:
        response = await self.client.post(
            f"{self.identity}/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": secret,
                "scope": scope,
            },
        )
        body = await require_status(response, 200)
        return str(body["access_token"])

    @staticmethod
    def business_headers(token: str, correlation: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}", "X-Correlation-Id": correlation}

    async def configure_subscription(
        self,
        base_url: str,
        token: str,
        event_type: str,
        key: str,
    ) -> None:
        response = await self.client.post(
            f"{base_url}/v1/webhook-subscriptions",
            headers=self.business_headers(token, "case-platform-webhooks")
            | {"Idempotency-Key": key},
            json={
                "eventTypes": [event_type],
                "targetUrl": "http://webhook-receiver:8080/events",
            },
        )
        body = await require_status(response, 201)
        secret = str(body.pop("secret"))
        configured = await self.client.post(
            f"{self.receiver}/internal/v1/secrets",
            headers=self.receiver_headers,
            json={"secret": secret},
        )
        await require_status(configured, 204)

    async def prepare(self) -> None:
        reset = await self.reset()
        cleared = await self.client.post(
            f"{self.receiver}/internal/v1/reset",
            headers=self.receiver_headers,
        )
        await require_status(cleared, 204)
        manager = await self.token("webhook-manager", "webhook-secret", "webhooks:manage")
        await self.configure_subscription(
            self.identity,
            manager,
            "identity.token.issued",
            "identity-subscription-1",
        )
        await self.configure_subscription(
            self.crm,
            manager,
            "crm.note.created",
            "crm-subscription-1",
        )
        support = await self.token(
            "support-agent",
            "support-secret",
            "crm:read crm:notes:write",
        )
        headers = self.business_headers(support, "case-platform-success")
        me = await self.client.get(f"{self.identity}/v1/me", headers=headers)
        await require_status(me, 200)
        self.record(self.success_calls, "identity.me", me)
        search = await self.client.get(
            f"{self.crm}/v1/customers",
            params={"email": "alex.unique@example.test"},
            headers=headers,
        )
        search_body = await require_status(search, 200)
        self.record(self.success_calls, "crm.customer.search", search)
        items = list(search_body["items"])
        if [item["customerId"] for item in items] != ["cus_unique"]:
            raise AssertionError("unique CRM search did not return one expected customer")
        customer = await self.client.get(f"{self.crm}/v1/customers/cus_unique", headers=headers)
        await require_status(customer, 200)
        self.record(self.success_calls, "crm.customer.get", customer)
        note_headers = headers | {
            "Idempotency-Key": "platform-success-note",
            "If-Match": customer.headers["ETag"],
        }
        note_payload = {"body": "Customer prefers email", "association": "account"}
        created = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=note_headers,
            json=note_payload,
        )
        created_body = await require_status(created, 201)
        self.record(self.success_calls, "crm.note.create", created)
        replay = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=note_headers,
            json=note_payload,
        )
        replay_body = await require_status(replay, 201)
        self.record(self.success_calls, "crm.note.replay", replay)
        if replay_body != created_body or replay.headers["Idempotency-Replayed"] != "true":
            raise AssertionError("CRM note replay differed from the original result")
        received: list[dict[str, Any]] = []
        for _attempt in range(30):
            response = await self.client.get(
                f"{self.receiver}/internal/v1/events",
                headers=self.receiver_headers,
            )
            received = list(await require_status(response, 200))
            event_types = {json.loads(str(item["body"]))["eventType"] for item in received}
            if {"identity.token.issued", "crm.note.created"}.issubset(event_types):
                break
            advanced = await self.client.post(
                f"{self.control}/control/v1/time/advance",
                headers=self.control_headers,
                json={"duration": "PT1S"},
            )
            await require_status(advanced, 200)
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("signed Identity and CRM events were not delivered")
        if not all(item["signatureValid"] is True for item in received):
            raise AssertionError("receiver observed an invalid signature")
        self.write("successful-calls.json", self.success_calls)
        self.write("webhook-transcript.json", received)
        self.write(
            "prepare-state.json",
            {
                "customerId": "cus_unique",
                "noteId": created_body["noteId"],
                "scenarioEpoch": reset["scenarioEpoch"],
                "manifestChecksum": reset["manifestChecksum"],
            },
        )
```

The transcript recorder stores no headers or token responses. Subscription
secrets are removed from response bodies before any artefact write.

- [ ] **Step 5: Implement restart and failure phases**

```python
class Driver:
    async def after_restart(self) -> None:
        saved = json.loads((self.artifacts / "prepare-state.json").read_text(encoding="utf-8"))
        support = await self.token(
            "support-agent",
            "support-secret",
            "crm:read crm:notes:write",
        )
        response = await self.client.get(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=self.business_headers(support, "case-platform-restart"),
        )
        body = await require_status(response, 200)
        survived = saved["noteId"] in {item["noteId"] for item in body["items"]}
        if not survived:
            raise AssertionError("CRM note did not survive application restart")
        self.write("restart.json", {"stateSurvived": True, "noteId": saved["noteId"]})

    async def failures(self) -> None:
        initial_reset = await self.reset()
        missing = await self.client.get(
            f"{self.crm}/v1/customers",
            headers={"X-Correlation-Id": "case-failure-missing-token"},
        )
        await require_status(missing, 401)
        self.record(self.failure_calls, "auth.missing", missing)
        read_only = await self.token("read-only-evaluator", "evaluator-secret", "crm:read")
        read_headers = self.business_headers(read_only, "case-failure-read-only")
        allowed_read = await self.client.get(
            f"{self.crm}/v1/customers/cus_unique",
            headers=read_headers,
        )
        await require_status(allowed_read, 200)
        self.record(self.failure_calls, "auth.read-only-read", allowed_read)
        forbidden = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=read_headers | {"Idempotency-Key": "forbidden-note", "If-Match": '"1"'},
            json={"body": "must not be written", "association": "account"},
        )
        await require_status(forbidden, 403)
        self.record(self.failure_calls, "auth.read-only-write", forbidden)
        support = await self.token(
            "support-agent",
            "support-secret",
            "crm:read crm:notes:write",
        )
        headers = self.business_headers(support, "case-platform-failures")
        ambiguous = await self.client.get(
            f"{self.crm}/v1/customers",
            params={"email": "shared@example.test"},
            headers=headers,
        )
        ambiguous_body = await require_status(ambiguous, 200)
        ambiguous_ids = [item["customerId"] for item in ambiguous_body["items"]]
        if ambiguous_ids != ["cus_ambiguous_a", "cus_ambiguous_b"]:
            raise AssertionError("ambiguous CRM search chose or reordered a customer")
        self.record(self.failure_calls, "crm.search.ambiguous", ambiguous)
        payload = {"body": "idempotency baseline", "association": "account"}
        stale_headers = headers | {
            "Idempotency-Key": "reusable-after-stale",
            "If-Match": '"0"',
        }
        stale = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=stale_headers,
            json=payload,
        )
        await require_status(stale, 409)
        self.record(self.failure_calls, "crm.note.stale", stale)
        valid_headers = stale_headers | {"If-Match": '"1"'}
        valid = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=valid_headers,
            json=payload,
        )
        await require_status(valid, 201)
        self.record(self.failure_calls, "crm.note.valid-after-stale", valid)
        changed = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=valid_headers,
            json={"body": "changed under same key", "association": "account"},
        )
        await require_status(changed, 409)
        self.record(self.failure_calls, "crm.note.idempotency-mismatch", changed)
        manager = await self.token("webhook-manager", "webhook-secret", "webhooks:manage")
        invalid_target = await self.client.post(
            f"{self.crm}/v1/webhook-subscriptions",
            headers=self.business_headers(manager, "case-invalid-target")
            | {"Idempotency-Key": "invalid-target"},
            json={"eventTypes": ["crm.note.created"], "targetUrl": "http://127.0.0.1/events"},
        )
        await require_status(invalid_target, 422)
        self.record(self.failure_calls, "webhook.target.denied", invalid_target)
        fault = await self.client.post(
            f"{self.control}/control/v1/faults",
            headers=self.control_headers,
            json={
                "ruleId": "crm-note-timeout-once",
                "targetService": "crm",
                "operation": "crm.note.create",
                "phase": "after_commit",
                "effect": "timeout",
                "actorId": "person-support-1",
                "occurrence": 1,
                "activationCount": 1,
                "delayMs": 250,
            },
        )
        await require_status(fault, 201)
        current = await self.client.get(
            f"{self.crm}/v1/customers/cus_unique",
            headers=headers,
        )
        await require_status(current, 200)
        timeout_headers = headers | {
            "Idempotency-Key": "timeout-note",
            "If-Match": current.headers["ETag"],
        }
        timeout_payload = {"body": "committed before timeout", "association": "account"}
        try:
            await self.client.post(
                f"{self.crm}/v1/customers/cus_unique/notes",
                headers=timeout_headers,
                json=timeout_payload,
                timeout=0.05,
            )
        except httpx.TimeoutException:
            pass
        else:
            raise AssertionError("injected after-commit timeout was not observed")
        await asyncio.sleep(0.3)
        notes = await self.client.get(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=headers,
        )
        notes_body = await require_status(notes, 200)
        timeout_notes = [item for item in notes_body["items"] if item["body"] == timeout_payload["body"]]
        if len(timeout_notes) != 1:
            raise AssertionError("after-commit timeout did not leave exactly one note")
        replay = await self.client.post(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=timeout_headers,
            json=timeout_payload,
        )
        replay_body = await require_status(replay, 201)
        if replay_body["noteId"] != timeout_notes[0]["noteId"]:
            raise AssertionError("timeout retry did not return the committed note")
        self.record(self.failure_calls, "crm.note.timeout-reconciled", replay)
        activations = await self.client.get(
            f"{self.control}/control/v1/fault-activations",
            headers=self.control_headers,
        )
        activation_body = await require_status(activations, 200)
        if len(activation_body) != 1:
            raise AssertionError("expected exactly one fault activation")
        final_reset = await self.reset()
        if final_reset["manifestChecksum"] != initial_reset["manifestChecksum"]:
            raise AssertionError("same-seed reset checksum changed")
        new_support = await self.token(
            "support-agent",
            "support-secret",
            "crm:read crm:notes:write",
        )
        after = await self.client.get(
            f"{self.crm}/v1/customers/cus_unique/notes",
            headers=self.business_headers(new_support, "case-after-failure-reset"),
        )
        after_body = await require_status(after, 200)
        if after_body["items"]:
            raise AssertionError("same-seed reset retained test-created notes")
        self.write("failure-calls.json", self.failure_calls)
        self.write("fault-activations.json", activation_body)
        self.write(
            "reset-checksums.json",
            [initial_reset["manifestChecksum"], final_reset["manifestChecksum"]],
        )

    def summarise(self) -> None:
        required = [
            "successful-calls.json",
            "webhook-transcript.json",
            "prepare-state.json",
            "restart.json",
            "failure-calls.json",
            "fault-activations.json",
            "reset-checksums.json",
        ]
        missing = [name for name in required if not (self.artifacts / name).is_file()]
        if missing:
            raise AssertionError(f"conformance artefacts are missing: {missing}")
        self.write(
            "summary.json",
            {
                "status": "passed",
                "successfulSequence": "passed",
                "failureSequence": "passed",
                "restartPersistence": "passed",
                "webhookSignatures": "passed",
            },
        )


async def run(phase: str) -> None:
    driver = Driver()
    try:
        if phase == "prepare":
            await driver.prepare()
        elif phase == "after-restart":
            await driver.after_restart()
        elif phase == "failures":
            await driver.failures()
        elif phase == "summarise":
            driver.summarise()
        else:
            raise ValueError(f"unknown conformance phase: {phase}")
    finally:
        await driver.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["prepare", "after-restart", "failures", "summarise"])
    asyncio.run(run(parser.parse_args().phase))
```

- [ ] **Step 6: Add the host restart wrapper without a Docker socket mount**

```bash
#!/usr/bin/env bash
# scripts/conformance
set -euo pipefail

command_name="${1:-}"
if [[ "$command_name" != "platform-contracts" ]]; then
  echo "usage: ./scripts/conformance platform-contracts" >&2
  exit 2
fi

mkdir -p artifacts/platform-contracts
docker compose --profile test up -d --build --wait
docker compose run --rm conformance python -m enterprise_twins.conformance.platform_contracts prepare
docker compose restart crm
docker compose up -d --wait crm
docker compose run --rm conformance python -m enterprise_twins.conformance.platform_contracts after-restart
docker compose run --rm conformance python -m enterprise_twins.conformance.platform_contracts failures
docker compose run --rm conformance python -m enterprise_twins.conformance.platform_contracts summarise
```

Add these test-profile services to Compose:

```yaml
  webhook-receiver:
    profiles: [test]
    build: .
    command:
      - uvicorn
      - enterprise_twins.conformance.receiver:app
      - --host
      - 0.0.0.0
      - --port
      - "8080"
    environment:
      TWINS_RECEIVER_CONTROL_TOKEN: participant-local-token
    networks: [twin-webhook-egress, twin-control]

  conformance:
    profiles: [test]
    build:
      context: .
      target: test
    environment:
      IDENTITY_URL: http://identity:8000
      CRM_URL: http://crm:8000
      CONTROL_URL: http://control:8000
      CONTROL_TOKEN: controller-local-token
      RECEIVER_URL: http://webhook-receiver:8080
      RECEIVER_TOKEN: participant-local-token
      ARTIFACT_ROOT: /app/artifacts/platform-contracts
    volumes:
      - ./artifacts:/app/artifacts
    command: ["python", "-c", "import time; time.sleep(10**9)"]
    depends_on:
      identity: {condition: service_healthy}
      crm: {condition: service_healthy}
      webhook-receiver: {condition: service_started}
    networks: [twin-public, twin-control]

  public-probe:
    profiles: [test]
    build: .
    entrypoint: []
    command: ["sleep", "infinity"]
    networks: [twin-public]
```

The `conformance` service uses the test image, mounts only
`./artifacts:/app/artifacts`, and has no Docker socket. It receives business,
Control, and receiver URLs plus private test tokens through environment
variables. `summarise` refuses to emit `passed` unless every expected assertion
and artefact exists.

- [ ] **Step 7: Document exact operator commands and boundaries**

Write `docs/development.md` with these tested commands:

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

Document the published Identity and CRM loopback URLs, the four network
memberships, synthetic credential warning, reset semantics, virtual time rule,
artefact paths, and the fact that this slice proves platform contracts rather
than the refund workflow.

- [ ] **Step 8: Run the full proving-slice verification**

Run:

```bash
chmod +x scripts/conformance scripts/update-scenario-checksums
uv sync --locked --all-groups
uv run ruff check .
uv run ruff format --check .
uv run mypy
docker compose config --quiet
docker compose up -d --build --wait
docker compose run --rm test-runner pytest tests/unit tests/contract tests/integration -q
uv run pytest tests/conformance/test_platform_script.py -q
```

Expected: all static checks and tests pass; the wrapper exits 0; the summary and five evidence files exist; no public test uses a database URL.

- [ ] **Step 9: Inspect public contracts and network exposure**

Run:

```bash
curl --fail http://127.0.0.1:8101/openapi.json > /tmp/identity-openapi.json
curl --fail http://127.0.0.1:8102/openapi.json > /tmp/crm-openapi.json
docker compose ps --format json
docker compose run --rm public-probe sh -c '! getent hosts control'
```

The final command uses a public-only probe because the conformance driver
intentionally joins `twin-control`. Expected: both OpenAPI documents declare 3.1; only ports 8101
and 8102 are published; the public-only probe cannot resolve Control.

- [ ] **Step 10: Commit the verified slice**

```bash
git add src/enterprise_twins/conformance scripts/conformance compose.yaml tests/conformance docs/development.md
git commit -m "test: prove platform contracts through public APIs"
```

Runtime artefacts remain ignored. They may contain request data and are not
committed. Bearer tokens, client secrets, and webhook secrets must not appear
in the redacted artefacts.
