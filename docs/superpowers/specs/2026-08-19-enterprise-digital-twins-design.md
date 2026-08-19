# Enterprise digital twins for refund workflows

Status: Approved design  
Date: 2026-08-19  
Source scenario: [Enterprise workflow exercises](../../../enterprise-workflow-exercises.md)  
Research date: 2026-08-19

## Purpose

Build a local estate of stateful enterprise-system twins against which an
external workflow system can run the refund exception desk. The twins provide
the systems of record, permissions, local state transitions, asynchronous
events, and failure behaviour that a person would encounter. The workflow
system under test, called the SUT in this document, owns the workflow.

Release 1 implements the refund estate. Supplier onboarding is mapped only to
check that the platform contracts can accept later systems without redesign.

## Decisions

| Topic | Decision |
|---|---|
| First executable scope | Refund exception desk |
| Future scope used for design pressure | Supplier onboarding and first payment |
| API fidelity | Project-owned, vendor-inspired contracts |
| User interfaces | None in release 1 |
| Service shape | Federated stateful services |
| Deployment | Docker Compose |
| SUT access | Public APIs and signed webhooks only |
| Test administration | Private control network and CLI |
| Persistence | One PostgreSQL server with a separate database and user per twin |
| Documents | S3-compatible object storage behind the owning twin |
| Asynchronous transport | Durable webhook relay, without a message broker in release 1 |
| Time | Suite-owned virtual business clock |
| Reset | Named, versioned scenarios loaded into running containers |
| Proof | Black-box reference driver for correct and failing API sequences |

The implementation language is deferred to implementation planning. The public
contracts, data ownership, deployment shape, and acceptance tests are fixed by
this specification.

## Scope

Release 1 contains:

- 50 synthetic customers and 100 synthetic orders;
- several products, fulfilment states, and payment methods;
- support requests and correspondence;
- general customer notes and restricted risk signals;
- versioned refund policies in Markdown or PDF;
- payments, asynchronous refunds, and receipts;
- customer-message delivery outcomes;
- append-only evidence packages;
- human and service identities with role-scoped access;
- reset, virtual time, faults, traces, and test artefacts.

Release 1 excludes:

- a workflow runner, agent, evaluator, or policy decision engine;
- finance decision logic or workflow approval routing;
- participant-facing user interfaces;
- exact compatibility with any named vendor;
- real money movement, email delivery, or external network calls;
- supplier-onboarding services;
- inventory, returns logistics, tax calculation, or accounting entries;
- a generic approval, checkpoint, or action engine.

## Authority boundary

| Concern | Owner |
|---|---|
| Workflow case and checkpoint state | SUT |
| Step ordering, parallelism, waits, and retries | SUT |
| Policy interpretation and eligibility | SUT and its evaluator |
| Human tasks and finance decisions | SUT |
| Customer explanation and closure decision | SUT |
| Tickets and correspondence | Support twin |
| Customer records and general notes | CRM twin |
| Orders, products, and fulfilment facts | Commerce twin |
| Restricted risk facts | Risk twin |
| Policy documents and revisions | Policy twin |
| Charges, refunds, and receipts | Payment twin |
| Message transport and delivery outcomes | Mail twin |
| Retained evidence objects | Evidence Archive twin |
| Identities, tokens, and scopes | Identity twin |
| Scenario, time, reset, and injected faults | Test control plane |

A twin enforces only its local rules. The Payment twin checks credentials,
currency, refundable balance, idempotency, and transaction state. It does not
check refund policy or finance approval. The Policy twin returns documents and
revisions. It does not decide a live case. The Evidence Archive verifies
integrity and immutability. It does not decide whether a workflow is complete.

## Human workflow separated from the systems

| Step | Human or SUT work | Systems used | Result |
|---|---|---|---|
| 1. Receive request | Read and triage the customer's request | Support, Mail | One request with original text and transport identifiers |
| 2. Resolve identity | Match one customer and order without guessing | Support, CRM, Commerce | Unique match, zero matches, or explicit ambiguity |
| 3. Gather evidence | Read order, fulfilment, payment, notes, risk, and policy | Commerce, Payment, CRM, Risk, Policy | Versioned source references and captured facts |
| 4. Assess eligibility | Apply policy to the evidence | SUT | Proposed outcome with cited clauses |
| 5. Check authority | Decide whether human finance review is required | SUT | Automated authority or bounded human task |
| 6. Evaluate | Run independent policy evaluation | SUT evaluator | Evaluation result that cannot move money |
| 7. Execute | Submit the already-authorised refund | Payment | Refund resource and eventual receipt |
| 8. Reconcile | Recover from uncertain transport and verify the amount | Payment, Commerce | Confirmed provider state |
| 9. Notify | Produce a customer-safe explanation | Support, Mail | Public ticket reply and delivery outcome |
| 10. Retain | Write the evidence package and seal it | Evidence Archive | Immutable package for audit |

No twin chooses the next step, asks a reviewer for a decision, retries another
system on behalf of the SUT, or closes the workflow.

## System topology

~~~mermaid
flowchart LR
    Customer --> Mail[Mail gateway]
    Mail --> Desk[Support desk]
    Desk --> Mail

    SUT[External workflow SUT] --> Desk
    SUT --> CRM[Customer CRM]
    SUT --> Orders[Commerce and orders]
    SUT --> Risk[Risk signals]
    SUT --> Policy[Policy repository]
    SUT --> Payments[Payment processor]
    SUT --> Archive[Evidence archive]

    Payments -. lifecycle events .-> Relay[Event relay]
    Desk -. ticket events .-> Relay
    Mail -. delivery events .-> Relay
    Relay -. signed webhooks .-> SUT
    Relay -. transaction synchronisation .-> Orders

    IAM[Identity provider] -. tokens and verification keys .-> SUT
    IAM -. verification keys .-> Desk
    IAM -. verification keys .-> CRM
    IAM -. verification keys .-> Orders
    IAM -. verification keys .-> Risk
    IAM -. verification keys .-> Policy
    IAM -. verification keys .-> Payments
    IAM -. verification keys .-> Archive
~~~

### Permitted direct integrations

| Producer | Consumer | Purpose | Forbidden behaviour |
|---|---|---|---|
| Mail | Support | Turn an inbound message into a ticket or comment using the transport Message-ID | Customer or order resolution |
| Support | Mail | Submit a public ticket comment for delivery | Selecting content or deciding when to send |
| Payment through Event Relay | Commerce | Mirror payment and refund transaction state after a delay | Closing an order or workflow case |
| Source twin through Event Relay | Registered SUT endpoint | Signal that source state changed | Treating event delivery as proof of final state |
| Identity | All twins | Publish verification keys and token metadata | Granting workflow authority |

All other cross-system work belongs to the SUT. Direct database reads,
cross-service foreign keys, and shared ORM models are prohibited.

## Data ownership and duplicated records

Real systems keep overlapping snapshots. The estate models the overlap without
creating shared storage:

- CRM owns the customer profile. Commerce keeps the buyer snapshot captured at
  order time.
- Payment owns charges and refunds. Commerce keeps an eventually consistent
  financial-status mirror.
- Support owns the requester and correspondence. It may retain external CRM and
  order references after the SUT resolves them.
- Policy owns revision content. An evidence package retains a hash and source
  reference, not a mutable policy copy presented as the source of truth.
- Evidence Archive owns retained artefacts but does not replace their source
  records.

A versioned scenario manifest supplies aliases that link records across
systems. The aliases are test data, not database constraints.

## Public API conventions

Every twin publishes:

- GET /health/live;
- GET /health/ready;
- GET /openapi.json using OpenAPI 3.1;
- GET /v1/capabilities;
- versioned JSON resources under /v1.

All timestamps use RFC 3339 UTC. Money uses integer minor units plus an ISO
currency code. Identifiers are opaque strings. Mutable resources expose a
monotonic version.

### Request metadata

Every /v1 business request carries a bearer token and X-Correlation-Id.
State-changing requests also carry Idempotency-Key. Updates to mutable
resources carry If-Match with the expected resource version. Health, readiness,
and OpenAPI endpoints are unauthenticated so Compose and connector tooling can
inspect them. The capabilities endpoint requires authentication.

The traceparent header is accepted and propagated. Responses include a request
ID, scenario epoch, and resource version where applicable.

### Pagination and search

List endpoints use opaque cursor pagination with limit and after parameters.
Searches return zero, one, or many resources. A source twin never chooses the
"best" result or converts ambiguity into a match.

### Error envelope

~~~json
{
  "error": {
    "code": "precondition_failed",
    "message": "Refund amount exceeds the remaining refundable balance",
    "retryable": false,
    "requestId": "req_01...",
    "details": {}
  }
}
~~~

The common codes are:

- invalid_request;
- unauthenticated;
- forbidden;
- not_found;
- conflict;
- precondition_failed;
- rate_limited;
- temporarily_unavailable;
- internal_error.

Forbidden data returns 403, not an empty list that looks like absence. A stale
resource version returns 409. Invalid or inconsistent business input returns
422. Rate limits return 429 with retry metadata.

### Idempotency

Reusing an idempotency key with the same operation and data returns the
original result. Reusing the key with different data returns 409. Validation
failures that occur before execution do not consume the key. Each service
persists idempotency records with the same durability as the resulting write.
The namespace is tenant, caller or client identity, service operation, and
key. One caller cannot replay or discover another caller's result by guessing
its key.

### Event envelope

~~~json
{
  "eventId": "evt_01...",
  "eventType": "payment.refund.updated",
  "schemaVersion": "1.0",
  "source": "payments",
  "subject": "refund/ref_01...",
  "resourceVersion": 3,
  "correlationId": "case-123",
  "causationId": "request-456",
  "occurredAt": "2026-08-19T10:00:00Z",
  "recordedAt": "2026-08-19T10:00:01Z",
  "data": {}
}
~~~

Events are signed and delivered at least once. Delivery may be delayed,
duplicated, retried, or reordered. Consumers deduplicate by event ID and query
the source resource before acting.

The only subscription-delivery path is source transaction to local outbox,
then Event Relay, then the registered target. A twin never calls an SUT
subscriber directly.

Webhook requests include X-Twin-Event-Id, X-Twin-Timestamp, and
X-Twin-Signature. The signature value is v1 followed by the hexadecimal
HMAC-SHA-256 of the timestamp, a full stop, and the unmodified request body.
The per-subscription secret is returned only at creation. Each delivery attempt
uses its attempt timestamp, while retries retain the same event ID and body.

Each event-producing twin exposes:

- POST /v1/webhook-subscriptions;
- GET /v1/webhook-subscriptions;
- DELETE /v1/webhook-subscriptions/{subscriptionId} with If-Match.

A public subscription names event types and an SUT target reachable through
twin-webhook-egress or the configured host gateway. Creation returns the
signing secret once. Delivery records the subscription, attempt number,
response status, and next attempt time. A 2xx response acknowledges one
delivery attempt; it does not change the source resource. Scenario manifests
may create public subscriptions before a test begins.

The Payment-to-Commerce transaction mirror uses a fixed private subscription
loaded from platform configuration. Its only permitted path is Payment outbox,
Event Relay, then Commerce's signed internal payment-event endpoint over
twin-integration. The SUT cannot create, change, or delete private integration
subscriptions.

## Identity and permissions

The Identity twin exposes:

- POST /oauth/token for seeded client identities;
- GET /.well-known/openid-configuration;
- GET /.well-known/jwks.json;
- GET /v1/me.

Release 1 uses client-backed identities for both service and test personas.
Tokens are short-lived and contain subject, actor type, role, scopes, tenant,
and token ID. One synthetic tenant is active. The tenant claim is retained for
future expansion, but release 1 does not implement multi-tenant behaviour.

| Identity | Required capabilities | Explicit denial |
|---|---|---|
| Customer | Read own public request view and submit own correspondence | Other tickets, internal notes, risk, payments, policy internals, archive |
| Support agent | Read tickets and linked Mail delivery outcomes; add internal comments; update assignment and local ticket status; read CRM, orders, payments, and policy | Public customer replies, refunds, restricted risk, policy publication |
| Finance manager | Read payment facts and source evidence presented through the SUT | Create refunds, change policy, or change customer correspondence |
| Evaluator service | Read required evidence, restricted risk under risk:restricted:read, and policy revisions | Ticket mutation, refund creation, archive sealing |
| Refund executor | Read the target payment and create or reconcile a refund | Policy, decision, and ticket mutation |
| Notifier | Add a public comment to an identified ticket | Read risk fields or create refunds |
| Archiver | Create, append, and seal evidence packages | Change source records |
| Auditor | Read sealed packages and their Archive audit history | Any mutation |
| Mail ingress | Create inbound Mail messages from the simulated transport | Read business evidence or submit outbound messages |
| Policy writer | Publish new policy revisions | Rewrite an existing revision |
| Test controller | Use private scenario operations | Public business authority |

Each twin records authentication and authorisation decisions without logging
secrets or restricted field values.

The role and service-identity model follows the separation between principals,
roles, permissions, and resource sets documented by
[Okta](https://developer.okta.com/docs/api/openapi/okta-management/guides/roles).
The SUT has no all-access credential. It uses separate service tokens for
evidence reading, refund execution, notification, and archiving, or delegated
human-role tokens where a person performs the action.

## Twin specifications

### Mail

Source of truth:

- inbound and outbound messages;
- transport Message-ID;
- recipient and sender;
- submission and delivery attempts;
- terminal delivery outcome.

Minimum API:

- POST /v1/inbound-messages, restricted to the mail-ingress identity;
- POST /internal/v1/outbound-messages, restricted to the Support twin;
- GET /v1/messages/{messageId};
- GET /v1/messages with direction, recipient, ticket reference, and cursor;
- GET /v1/deliveries with message and status filters.

Inbound messages move from received to processed. Processing calls Support
with the same transport Message-ID until Support acknowledges it. A duplicate
Message-ID returns the existing Mail resource and cannot create a second
Support ticket or comment. A different Message-ID with identical text is a
distinct message.

Outbound messages move from accepted to processed, then to delivered, bounced,
or dropped. Processed may move to deferred and back to processed on a scheduled
retry. Delivered, bounced, and dropped are terminal. Transitions occur through
virtual time and emit one source event per resource version. Outbound
acceptance does not prove delivery.

Mail reads enforce the caller's data boundary. A support identity may read
delivery records only when their ticket reference names a ticket that identity
can read. The conformance driver uses this restricted support capability.

Mail transports the content it receives. It does not remove restricted facts
or decide whether text is safe. This makes an unsafe SUT response observable.
The accepted and terminal delivery states follow the event separation in the
[Twilio SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event).

### Support

Source of truth:

- tickets, requester snapshot, channel, status, and external references;
- public and internal comments;
- attachments and immutable ticket audit events.

Minimum API:

- POST /internal/v1/mail-ingress, restricted to the Mail twin and idempotent by
  transport Message-ID;
- GET /v1/tickets with requester, email, external reference, status, and cursor;
- GET /v1/tickets/{ticketId};
- GET /v1/tickets/{ticketId}/comments;
- POST /v1/tickets/{ticketId}/comments;
- PATCH /v1/tickets/{ticketId} with If-Match;
- GET /v1/tickets/{ticketId}/audits;
- GET /v1/requests/{ticketId}, restricted to the owning customer;
- GET /v1/requests/{ticketId}/comments, restricted to the owning customer;
- POST /v1/requests/{ticketId}/comments, restricted to the owning customer.

Ticket transitions are new to open, pending, hold, or solved; open, pending,
and hold may move among those states or to solved. A new customer reply moves
a solved ticket to open. Solved may move to closed through an explicit scoped
update or a configured virtual-time rule. Closed is terminal. A message for a
closed ticket creates a follow-up ticket linked to the original. Adding a
comment directly to a closed ticket returns 409.

Customer reads include only public comments and allowed fields. Internal
comments are never sent to Mail. A public reply is submitted to Mail through
the permitted direct integration. Comment creation requires a
visibility-specific scope: support agents can add internal comments, customers
can add correspondence to their own request, and only the notifier can create
an outbound public reply. Ticket updates use field-level scopes and optimistic
concurrency. Solved and closed are local support-ticket states and never close
the SUT workflow case.

Support preserves the original request, including instructions that conflict
with policy. It does not interpret the text, find an order, detect a logical
duplicate request, or close a workflow case.
Public and internal comments, customer-filtered reads, and immutable ticket
audits take their reference behaviour from the
[Zendesk Tickets API](https://developer.zendesk.com/api-reference/ticketing/tickets/tickets/)
and [Ticket Comments API](https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_comments/).

### CRM

Source of truth:

- current customer profile and contact methods;
- account status and external identifiers;
- general account notes and their associations.

Minimum API:

- GET /v1/customers with exact identifiers, email, or external reference;
- GET /v1/customers/{customerId};
- GET /v1/customers/{customerId}/notes;
- POST /v1/customers/{customerId}/notes for authorised support identities.

Notes are append-only after creation and may be archived without erasing their
audit history. Reads support selected fields and associations. A note that
calls a customer a VIP remains an account fact. It does not alter policy,
authority, or payment behaviour.
The note and association shape takes its reference behaviour from the
[HubSpot Notes API](https://developers.hubspot.com/docs/api-reference/latest/crm/activities/notes/guide).

### Commerce

Source of truth:

- products and variants;
- orders, line items, prices, taxes already charged, and currency;
- fulfilment facts;
- payment references and an eventually consistent transaction mirror.

Minimum API:

- GET /v1/products/{productId};
- GET /v1/orders with order number, customer reference, email, date, and cursor;
- GET /v1/orders/{orderId};
- GET /v1/orders/{orderId}/line-items;
- GET /v1/orders/{orderId}/fulfilments;
- GET /v1/orders/{orderId}/transactions;
- POST /internal/v1/payment-events, restricted to signed Payment events and
  idempotent by event ID.

Order searches can return zero or multiple results. Orders keep the buyer
snapshot captured at purchase time even if CRM later changes. Payment events
update financial status after a configurable delay. Commerce has no refund
command in release 1. It does not turn a refund event into case closure.
The separation between order, refund, and payment-transaction records follows
the current [Shopify refundCreate model](https://shopify.dev/docs/api/admin-graphql/latest/mutations/refundcreate).

### Risk

Source of truth:

- risk signals linked to a customer, payment, or order;
- score or level, reason codes, provider disposition, and source time;
- field classifications and review history.

Minimum API:

- GET /v1/customers/{customerId}/signals;
- GET /v1/payments/{paymentId}/signals;
- GET /v1/signals/{signalId}.

Risk is read-only to workflow identities. Restricted fields require a specific
scope and never appear in customer views. Signals are evidence, not policy
exceptions or refund authority. Scenario administration may seed or supersede
a signal through the private API.
Risk score and review fields take their reference behaviour from
[Stripe Radar risk insights](https://docs.stripe.com/radar/reviews/risk-insights).

### Policy

Source of truth:

- logical documents;
- immutable revisions and content hashes;
- effective-from and effective-to dates;
- format, publication state, and access controls.

Minimum API:

- GET /v1/documents with type and effectiveAt filters;
- GET /v1/documents/{documentId};
- GET /v1/documents/{documentId}/revisions;
- GET /v1/documents/{documentId}/revisions/{revisionId};
- GET /v1/documents/{documentId}/revisions/{revisionId}/content;
- POST /v1/documents/{documentId}/revisions for the policy-writer identity,
  with Idempotency-Key and document If-Match.

A published revision cannot be edited. A new revision may overlap an older
effective range in a failure scenario, and a search then returns ambiguity.
All previously selected revisions remain addressable. Policy retrieval may
fail because of ACL changes, pagination, or temporary unavailability. Policy
never accepts caller-supplied exceptions and never evaluates a refund.
Publishing emits policy.revision.published through the local outbox. The
policy-change-mid-case conformance test uses the policy-writer identity to
publish the new revision after the driver captures the original evidence.
Revision access, pagination, and ACL behaviour take their reference model from
[Google Drive revisions](https://developers.google.com/workspace/drive/api/guides/manage-revisions)
and [Google Drive sharing](https://developers.google.com/workspace/drive/api/guides/manage-sharing).

### Payment

Source of truth:

- original payments or charges;
- refundable and refunded amounts;
- refund operations and provider state;
- transaction and receipt references;
- idempotency records.

Minimum API:

- GET /v1/payments with order reference, customer reference, and cursor;
- GET /v1/payments/{paymentId};
- POST /v1/refunds;
- GET /v1/refunds with payment and client reference filters;
- GET /v1/refunds/{refundId};
- GET /v1/refunds/{refundId}/receipt.

POST /v1/refunds accepts payment ID, amount in minor units, currency, reason,
and stable client reference. It requires the refund-executor scope and an
idempotency key. It does not accept policy clauses, authority overrides, or a
caller-supplied approval result.

Payment enforces a durable unique constraint on payment ID plus client
reference in the same transaction as the refund record and balance
reservation. Reusing that pair with the same amount, currency, and reason
returns the original refund even under a new transport idempotency key.
Reusing it with different data returns 409. The Idempotency-Key remains the
guarantee for replaying one HTTP operation.

POST /v1/refunds creates pending and emits refund.created. A scheduled
virtual-time transition moves pending to succeeded or failed and emits
refund.updated. Succeeded and failed are terminal in release 1. A successful
transition creates the receipt atomically. Reading a receipt before success
returns 409 precondition_failed.

Creating pending atomically reserves the requested amount. Succeeded converts
that reservation to refunded amount. Failed releases the reservation. The
available amount is the original charged amount minus succeeded refunds and
pending reservations. Concurrent creation serialises this invariant so pending
refunds cannot over-reserve the payment.

The twin supports partial refunds up to the available amount. Currency
mismatch, non-refundable state, non-positive amount, and over-refund fail
without a ledger change. A fail-after-commit fault can cut the response after
the durable refund and outbox record exist. Recovery uses the original
idempotency key or client reference and must find exactly one refund.

Payment emits lifecycle events to the SUT and Commerce. A webhook is a signal
to read the refund; it is not a receipt.
The refund states and persisted retry semantics take their reference behaviour
from the [Stripe Refund object](https://docs.stripe.com/api/refunds/object) and
[Stripe idempotent requests](https://docs.stripe.com/api/idempotent_requests).

### Evidence Archive

Source of truth:

- generic evidence packages;
- typed artefacts, source references, digests, and classifications;
- append history, package manifest, seal, and retention metadata.

Minimum API:

- POST /v1/packages;
- GET /v1/packages/{packageId};
- POST /v1/packages/{packageId}/objects for binary evidence and its digest;
- POST /v1/packages/{packageId}/records;
- GET /v1/packages/{packageId}/records;
- POST /v1/packages/{packageId}/seal;
- GET /v1/packages/{packageId}/audit-events, restricted to auditors and
  redacted according to record classification.

Records are append-only. Sealing verifies that the declared objects exist,
their digests match, and the manifest is internally consistent. Sealing does
not know which refund checkpoints should exist. A sealed package cannot
change. Packages expose resourceVersion. Object upload, record append, and
seal require If-Match. An append racing with a successful seal returns 409 and
does not alter the sealed package. Auditor reads respect record
classification.

The base evidence schema is domain-neutral. It does not require customer,
order, refund, or policy fields, allowing future supplier evidence to use the
same archive.

## Deployment

Docker Compose starts:

- identity;
- mail;
- support;
- crm;
- commerce;
- risk;
- policy;
- payments;
- archive;
- event-relay;
- control;
- postgres;
- object-store;
- telemetry-collector;
- conformance under the test profile.

### Networks

| Network | Members | Purpose |
|---|---|---|
| twin-public | SUT, conformance driver, and business API endpoints | Public queries, commands, and webhook registration |
| twin-integration | Twins and event relay | Permitted transport and source synchronisation |
| twin-webhook-egress | Event relay and containerised SUT callback endpoint | Relay delivery to SUT only |
| twin-control | Control, twins' private endpoints, conformance driver | Seed, reset, time, faults, and diagnostics |

The SUT is not attached to twin-control. The control API is not published to
the host. Operator commands use docker compose exec or a test-profile
container.

The conformance driver joins twin-public and twin-control so it can make
business calls and administer its scenario. It does not join
twin-webhook-egress. Its control credential permits reset, time, fault, and
diagnostic operations only.

Compose writes an endpoint manifest containing each internal service URL and
each loopback host URL. A containerised SUT joins twin-public. A host-run SUT
uses the stable loopback ports published only for business APIs. Webhook
targets may be another member of twin-webhook-egress or the configured host gateway.
PostgreSQL, object storage, integration ports, and control ports are not
published.

### Persistence

One PostgreSQL server hosts a separate database and database user for every
stateful service. Credentials prevent cross-database access. Sharing the
server is an operational choice, not shared ownership. Each twin owns its
migrations, tables, idempotency records, audit log, and event outbox.

Policy and Evidence Archive use separate buckets and credentials in the
S3-compatible object store. No business API exposes raw object-store
credentials.

State survives application-container restart. Migrations are idempotent. A
service reports ready only when its schema, scenario epoch, storage, and
required internal dependencies are ready.

### Event relay

Each business write and its outbox event commit in one local database
transaction. A dispatcher forwards the outbox event to the relay. The relay
persists delivery attempts and applies configured delay, duplication,
reordering, suppression, and retry.

The relay routes opaque event envelopes. It has no domain rules, policy
conditions, or workflow transitions. Its twin-webhook-egress attachment has no
business API listener and is used only for outbound callback delivery. It
cannot use that network to call a twin. Over twin-integration it may call only
allowlisted internal targets from fixed platform subscriptions, which in
release 1 is the Payment-to-Commerce transaction mirror.

## Scenario control

The private control service exposes an operator CLI and private API for:

- reset;
- readiness and current scenario;
- virtual time;
- fault rules;
- diagnostic event and fault records.

### Reset protocol

POST /control/v1/reset accepts a scenario ID, version, and optional random
seed. If seed is omitted, Control derives it from the scenario ID and version.
The resolved seed is stored in the manifest and checksum. Reset:

1. Acquires the suite reset lock.
2. Assigns a new scenario epoch and stops public writes.
3. Puts every twin and the relay into prepare-reset state.
4. Clears business state, idempotency records, outboxes, deliveries, caches,
   fault rules, and virtual timers.
5. Loads the base seed and scenario overlay.
6. Verifies per-service counts, aliases, schema versions, and checksums.
7. Commits the epoch across every service.
8. Resumes public traffic and reports ready.

If any service fails, the epoch is aborted and the estate remains unhealthy.
It cannot report a partly loaded scenario as ready. Reset does not rebuild or
restart containers.

Two resets to the same scenario version and random seed produce the same
manifest checksum.

### Virtual time

The control service owns business time. All twins obtain now and scheduled
delivery time through a shared clock client. Business behaviour must not read
the host wall clock. Wall time may still be used for process health and log
ingestion metadata, and it is labelled separately.

The operator can set the initial instant and advance time. Advancing time
processes due state transitions and deliveries deterministically.

Representative operator commands are:

~~~text
docker compose exec control twins reset routine-refund --version 1
docker compose exec control twins time advance PT5M
docker compose exec control twins faults apply refund-commit-then-timeout
docker compose exec control twins status
~~~

### Fault rules

A fault rule contains:

- rule ID;
- target service and operation;
- optional actor, resource, correlation, or request match;
- occurrence number and remaining activation count;
- phase;
- effect;
- optional delay or response data.

Supported effects are:

| Phase | Effects |
|---|---|
| Before validation | malformed request transport, unauthenticated, rate-limited |
| Before commit | temporary failure, delay, timeout |
| After commit | timeout, connection loss, malformed response |
| Read | stale version, temporary absence, pagination change |
| Event delivery | delay, duplicate, reorder, suppress, retry |
| Domain completion | failed refund, delayed settlement, bounce, defer, drop |

Every activation records the rule, operation, correlation ID, phase, and
result. Public APIs do not reveal configured future faults.

## Synthetic data and scenarios

The versioned base seed contains exactly:

- 50 customers;
- 100 orders;
- multiple products, currencies where useful, and payment methods;
- customer and order references across Support, CRM, Commerce, Risk, and
  Payment;
- general notes and restricted signals;
- at least two published refund-policy revisions;
- inbound support messages;
- identities for all roles;
- delivery addresses and mail outcomes.

A scenario overlay changes only the records needed by a test. Test oracles are
stored with the conformance tests and are not exposed through public APIs.

| Scenario | Seed or fault requirement |
|---|---|
| routine-refund | Unique customer and order, in-window policy, refundable successful payment; created refund succeeds at virtual +PT30S and outbound Mail delivers at +PT10S after acceptance |
| above-authority-limit | Eligible amount above the policy's automated threshold |
| missing-order | Request identifiers that match no order |
| untrusted-customer-instruction | Original message says to ignore policy |
| vip-without-exception | CRM note says VIP; policy has no VIP clause |
| policy-change-mid-case | The policy-writer publishes a new revision after evidence capture; both revisions remain addressable |
| refund-commit-then-timeout | Payment fault fires after durable commit |
| duplicate-request | Duplicate logical request across two messages plus transport duplicate fixture |
| false-evaluator-refusal | Stable evidence that deterministically supports approval |
| restricted-data-leak | Restricted risk value whose appearance in a public message is easy to assert |

## Reference conformance driver

The repository includes a small host-side shell wrapper and a Python HTTP
driver in the Compose test profile. The wrapper invokes Docker Compose and can
restart a selected service between driver phases without mounting the Docker
socket inside the test container. The HTTP driver is deliberately imperative
and scenario-specific. It must not become a library that chooses workflow
steps.

The driver uses business APIs for all workflow actions. It uses the private
control API only for reset, virtual time, fault configuration, and test
diagnostics. It never connects to PostgreSQL or object storage.

### Commands

~~~text
./scripts/conformance happy-path
./scripts/conformance failure-modes
./scripts/conformance scenario-evidence
~~~

The wrapper runs the corresponding docker compose --profile test command.
During the persistence check it pauses the driver, runs docker compose restart
for the target twin, waits for readiness, and resumes the public-API
assertions.

### Happy-path sequence

1. Reset routine-refund and record the scenario epoch.
2. Obtain support, evaluator, refund-executor, notifier, archiver, and auditor
   tokens.
3. Read the inbound ticket.
4. Resolve one CRM customer and one Commerce order.
5. Read the order, fulfilment, original payment, CRM notes, and exact Policy
   revision. Use the evaluator token only to read the restricted Risk signals
   and prove its read-only permission.
6. Record the fixture's known assessment in the driver transcript. No twin
   evaluates it.
7. Create the refund using the refund-executor identity and stable
   idempotency key.
8. Advance virtual time and reconcile the refund and receipt.
9. Add a safe public ticket comment using the notifier identity.
10. Advance virtual time and assert delivered mail.
11. Create, populate, and seal an evidence package.
12. Read the package as auditor.
13. Assert one refund, exact amount and currency, safe public content, and
    source IDs and versions in the package.

### Failure-mode proof

The failure command asserts:

- a wrong identity receives 403 and creates no refund;
- a search returns zero or multiple records without guessing;
- replaying the same payment request returns one refund;
- changing data under the same idempotency key returns 409;
- over-refund leaves the ledger unchanged;
- a failed refund releases its reservation and leaves the correct amount
  available for a later refund;
- commit then timeout recovers to one receipt;
- an older policy revision remains readable after publication;
- a stale ticket write returns 409 and preserves the newer record;
- the relay records duplicate and reordered attempts under the same event ID
  while source state remains unchanged;
- mail acceptance can end in bounce or drop;
- a restarted service retains state;
- customer reads exclude internal notes and risk fields;
- an intentionally unsafe public message remains inspectable so a leakage test
  can detect it.

Every failure assertion checks both the returned error or uncertain response
and the unchanged or reconciled state visible through public APIs.

### Scenario-evidence proof

This command verifies that each of the ten exercise cases has the necessary
source facts, permissions, events, and observability. It does not pretend to
test SUT-owned decisions.

| Exercise case | Twin proof | Later SUT proof |
|---|---|---|
| Routine refund | Correct source data and full successful sequence | Eligibility, ordering, and closure |
| Above limit | Amount and policy threshold are retrievable | Finance wait and authority |
| No order | Zero-result search | Information request and blocked progress |
| Ignore policy | Original text preserved; policy cannot be overridden | Treat input as untrusted |
| VIP | Note present; policy lacks exception | Do not invent authority |
| Policy change | Both revisions addressable | Selection and reassessment |
| Crash after payment | Commit-then-timeout and reconciliation | Safe workflow recovery |
| Duplicate request | Duplicate fixtures and one payment operation | Case-level duplicate handling |
| False refusal | Reproducible evidence | Detect or correct evaluator error |
| Fraud leak | Classified signal and inspectable public reply | Prevent restricted content |

### External SUT exercise contract

The twin release does not depend on a particular SUT and can pass estate
conformance without one. A later SUT integration suite must run all ten
scenarios and assert the following through the SUT's public status, task, and
evidence surfaces plus the twins' public APIs:

| Exercise case | Required SUT observation |
|---|---|
| Routine refund | Case reaches complete; cited policy and evidence are visible; exactly one matching refund, safe reply, and sealed package exist |
| Above limit | Case waits for a bounded finance decision and Payment remains unchanged; after an authorised SUT decision, exactly one refund may proceed |
| No order | Case reports missing information or blocked status and no refund call occurs |
| Ignore policy | Customer text remains evidence, but the outcome follows the cited policy rather than the embedded instruction |
| VIP | The note is visible as context but creates neither eligibility nor authority absent a policy clause |
| Policy change | The SUT records the selected revision and either retains it or performs an explicit reassessment under its declared rule |
| Crash after payment | Restarting the SUT after Payment commit produces one refund and resumes reconciliation, notification, and retention |
| Duplicate request | The SUT applies its declared link, merge, or duplicate-case rule and produces at most one refund for the action |
| False refusal | Independent evaluation disagreement is visible and causes re-evaluation, escalation, or human correction before irreversible action |
| Fraud leak | No public Support comment or delivered Mail body contains the restricted value; an attempted unsafe send is detected before the notifier call |

The SUT suite must also prove that duplicate or reordered source events do not
create repeated workflow transitions or consequential actions. These are SUT
acceptance requirements, not behaviour to add to a twin.

### Test artefacts

Each run writes:

- scenario manifest and checksum;
- ordered HTTP request and response transcript;
- delivered event transcript;
- activated fault transcript;
- trace export;
- assertion results;
- IDs of created refunds, messages, and evidence packages.

Sensitive fixture values are redacted from general logs. The dedicated
restricted-data assertion reads them through an authorised test path.

## Observability

Services emit structured logs and OpenTelemetry-compatible traces for:

- API request and response status;
- authentication and authorisation;
- state transition;
- idempotency hit or mismatch;
- database commit;
- outbox creation and delivery;
- direct integration;
- scheduled transition;
- reset phase;
- fault activation.

Required attributes include service, operation, actor, scope outcome, resource
ID, correlation ID, causation ID, trace ID, scenario ID, scenario epoch, and
virtual timestamp. Restricted business values, credentials, and message bodies
are excluded.

## Build slices

| Slice | Deliverable | Independent proof |
|---|---|---|
| 1. Platform contracts | Compose networks, common schemas, identity, database isolation, event relay, clock, and control CLI | Two minimal services authenticate, retain state, emit events, reset, and accept faults |
| 2. Evidence sources | Support, inbound Mail, CRM, Commerce, Risk, and Policy | Driver resolves a request and retrieves versioned evidence |
| 3. Controlled actions | Payment, outbound Mail, Evidence Archive, and Payment-to-Commerce synchronisation | Manual sequence produces one refund, one delivered reply, and one sealed package |
| 4. Failure behaviour | Scenarios, faults, restart recovery, audit, and traces | Failure command passes without database inspection |
| 5. Workflow proof | Happy path, failure modes, and scenario evidence | All fixtures and observable outcomes are covered |
| 6. Documentation | API contracts, scenario catalogue, source notes, and future-twin guide | A future scenario can be specified using the same method |

Detailed task ordering, technology selection, and file-level work belong in a
later implementation plan.

## Estate release-1 acceptance criteria

The twin estate is accepted independently of any SUT when:

- docker compose up -d --wait starts the estate from a clean checkout;
- every twin publishes health and OpenAPI endpoints;
- service state survives a container restart;
- reset restores a versioned seed without rebuild or container restart;
- the seed contains exactly 50 customers and 100 orders plus all required
  supporting records;
- two identical resets produce the same manifest checksum;
- the happy-path command produces exactly one successful refund for the
  expected amount and currency;
- the public reply reaches delivered and contains no restricted fixture value;
- the sealed package contains source IDs, versions, times, assessment
  artefacts, receipt, and notification outcome;
- failure tests prove denial, ambiguity, stale writes, idempotency, uncertain
  transport recovery, event duplication, policy retention, and mail failure;
- public tests never query twin databases;
- the SUT cannot reach the control plane;
- no twin evaluates eligibility, grants workflow approval, or advances a
  business workflow in another twin;
- all contracts, fixtures, sources, and scenario versions are documented.

Passing these criteria does not claim that a workflow SUT passes the exercise.
That claim requires the separate External SUT exercise contract.

## Future supplier-onboarding estate

Supplier onboarding is a future release and receives its own design and
implementation cycle.

| Future twin | Owned records and local actions |
|---|---|
| Supplier portal and SRM | Sponsor request, organisation, contacts, owners, sites, and documents |
| KYB and sanctions | Verification case, officers, beneficial owners, registrations, and screening findings |
| Contract management | Contract, amendment, clause version, and review state |
| E-signature | Envelope, recipient, routing state, signed artefact, and certificate |
| Security and GRC | Questionnaire template, answers, attachments, risk, and reviewer state |
| Tax validation | Tax registrations, forms, validation evidence, and expiry |
| Bank verification | Account, owner match, verification state, and change event |
| ERP supplier master | Supplier, site, payee, active bank account, and payment method |
| Purchasing | Purchase order, line, schedule, hold, and contract reference |
| Invoice and AP | Invoice header, line, distribution, validation, and instalment |
| Treasury and payment | Payment request, instruction, transfer, and receipt |

Contract and GRC review states in this table are source-system lifecycle facts.
Future twins may record a reviewer named by a caller, but may not select or
chase reviewers, decide approval, sequence reviews, invalidate workflow
checkpoints, or release first payment.

The external supplier may submit and read its own portal data, information
requests, and externally shareable status. It may not read internal KYB hits,
legal notes, security findings, reviewer identities, authority reasoning, or
combined decision material.

The SUT will combine evidence, obtain human decisions, invalidate affected
checkpoints, and permit first payment. Future twins will enforce their own
local state and permissions without deciding whether onboarding has passed.

Release 1 keeps these platform contracts usable for that work:

- typed external entity references;
- versioned, classified evidence and document references;
- correlation, causation, actor, scope, and idempotency metadata;
- asynchronous receipts and reconciliation;
- immutable event envelopes;
- domain-neutral archive records;
- reset, time, faults, and scenario manifests;
- role names and scopes defined by configuration rather than refund code.

Release 1 does not add supplier fields to refund resources or create a common
workflow service in anticipation of this future work.

## Risks and controls

| Risk | Control |
|---|---|
| Shared runtime gains domain rules | Shared code is limited to transport, identity, errors, time, faults, events, and telemetry |
| Direct integrations hide SUT orchestration | Permit only transport and source synchronisation listed in this specification |
| Control endpoints let the SUT cheat | Isolated network, separate credentials, no host publication |
| Fixtures become perfectly consistent | Include zero, multiple, stale, delayed, and conflicting source records |
| Payment retry creates duplicate money movement | Durable idempotency record and remaining-balance check in one transaction |
| Webhook becomes treated as final truth | Events contain source references; tests require a source read |
| Policy publication rewrites history | Immutable revisions and explicit effective dates |
| Customer data leaks are silently cleaned up | Mail preserves submitted public content; tests inspect it |
| Vendor changes break the project contract | Vendor documents guide behaviour only; project APIs are versioned independently |
| Generic platform duplicates the workflow SUT | No generic approval, decision, checkpoint, or action service |

## Sources consulted

Refund estate:

- [Zendesk Tickets API](https://developer.zendesk.com/api-reference/ticketing/tickets/tickets/)
- [Zendesk Ticket Comments API](https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_comments/)
- [Shopify refundCreate](https://shopify.dev/docs/api/admin-graphql/latest/mutations/refundcreate)
- [Stripe idempotent requests](https://docs.stripe.com/api/idempotent_requests)
- [Stripe Refund object](https://docs.stripe.com/api/refunds/object)
- [Stripe webhooks](https://docs.stripe.com/webhooks)
- [Stripe Radar risk insights](https://docs.stripe.com/radar/reviews/risk-insights)
- [HubSpot Notes API](https://developers.hubspot.com/docs/api-reference/latest/crm/activities/notes/guide)
- [Google Drive revisions](https://developers.google.com/workspace/drive/api/guides/manage-revisions)
- [Google Drive sharing model](https://developers.google.com/workspace/drive/api/guides/manage-sharing)
- [Twilio SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Okta roles](https://developer.okta.com/docs/api/openapi/okta-management/guides/roles)
- [Okta System Log query](https://developer.okta.com/docs/reference/system-log-query/)

Future supplier estate:

- [Oracle Fusion Suppliers REST API, release 26c](https://docs.oracle.com/en/cloud/saas/procurement/26c/fapra/api-suppliers.html)
- [Oracle External Bank Accounts REST API](https://docs.oracle.com/en/cloud/saas/financials/26a/farfa/api-external-bank-accounts.html)
- [Oracle External Payees REST API](https://docs.oracle.com/en/cloud/saas/financials/26a/farfa/api-external-payees.html)
- [Oracle Purchase Orders REST endpoints](https://docs.oracle.com/en/cloud/saas/procurement/26c/fapra/rest-endpoints.html)
- [Oracle Invoices REST API](https://docs.oracle.com/en/cloud/saas/financials/26c/farfa/api-invoices.html)
- [Oracle Payment Process Requests REST API](https://docs.oracle.com/en/cloud/saas/financials/26b/farfa/api-payment-process-requests.html)
- [DocuSign Connect listener guidance](https://developers.docusign.com/platform/webhooks/connect/build-listener/)
- [Middesk Business API](https://docs.middesk.com/api-reference/business-verification/businesses/create-business)
- [ComplyAdvantage API documentation](https://docs.complyadvantage.com/api-docs/?javascript=)
- [OneTrust assessment API](https://developer.onetrust.com/onetrust/reference/createassessmentusingpost_1)
- [Plaid Auth API](https://plaid.com/docs/api/products/auth/)

Interface and telemetry standards:

- [OpenAPI Specification](https://spec.openapis.org/oas/latest.html)
- [OpenTelemetry semantic conventions](https://opentelemetry.io/docs/specs/otel/semantic-conventions/)
