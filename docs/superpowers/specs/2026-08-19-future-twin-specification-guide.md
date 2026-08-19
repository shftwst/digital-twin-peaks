# Guide for specifying future enterprise-system twins

Status: Approved reusable instruction set  
Date: 2026-08-19  
Reference design: [Enterprise digital twins for refund workflows](2026-08-19-enterprise-digital-twins-design.md)

## Purpose

Use this guide to turn a future business-workflow scenario into a buildable
specification for the enterprise-system twins around it. The output describes
systems that an external workflow SUT can query and command. It does not design
the workflow runner itself.

The method is domain-independent. It applies to finance, retail, procurement,
legal, security, customer service, logistics, and similar enterprise
environments.

## How to invoke the guide

Supply:

1. The future scenario document.
2. The release boundary, if already known.
3. Any named systems or protocols that require exact compatibility.
4. Deployment, language, regulatory, or data-location constraints.
5. The existing twin-platform specification and current contracts.

Then give this instruction:

~~~text
Read the future enterprise-twin specification guide and the supplied workflow
scenario. Separate the human workflow from the surrounding systems. Research
the relevant enterprise environment using current primary sources. Propose the
smallest useful release, map the independent systems and their APIs, define
stateful success and failure behaviour, and produce the specification described
by the guide. Do not assign workflow decisions or orchestration to a twin.
~~~

## Default decisions

Use these defaults unless the scenario or user changes them:

| Topic | Default |
|---|---|
| API fidelity | Vendor-inspired, project-owned API |
| Interface | REST and signed webhooks |
| Deployment | Docker Compose |
| Service shape | One stateful service per independent enterprise system |
| Persistence | Service-owned database and optional service-owned object bucket |
| Reset | Named, versioned scenario loaded into running containers |
| Time | Deterministic virtual business clock |
| Events | Durable, at-least-once delivery |
| Administration | Private control network, inaccessible to the SUT |
| Proof | Black-box manual-sequence driver plus failure tests |
| User interfaces | Excluded unless the scenario specifically tests them |

Do not silently retain a default when the scenario requires a different
choice. State the changed decision and its reason.

## Required process

### 1. Read the scenario and project instructions

Read the complete scenario, repository guidance, nearby design documents,
existing contracts, and recent project history. Record:

- the fixed business end;
- possible final outcomes;
- prohibited shortcuts;
- actors and authority limits;
- evidence and checkpoint requirements;
- test cases and recovery cases;
- data or regulatory constraints;
- what the external SUT is expected to own.

Do not start with a list of popular enterprise products. Start with the
workflow and its authority boundaries.

### 2. Select one executable release

If the scenario spans several independent business domains, split it into
releases. The first release must be independently runnable and testable.

For every deferred domain, record:

- why it is deferred;
- which current contract it may later reuse;
- which current design choice must remain domain-neutral;
- which services must not be built early.

Mapping future work is useful. Adding speculative services is not.

### 3. Clarify material choices

Ask only questions whose answers change system boundaries or acceptance.
Resolve them one at a time. Typical choices are:

- first executable workflow;
- vendor-inspired or exact vendor compatibility;
- API-only or replica user interfaces;
- data-volume and performance expectations;
- whether human approvals occur inside the SUT;
- required authentication protocol;
- whether virtual time is acceptable.

State a recommendation with each question.

### 4. Write the human workflow first

Describe the workflow as if a competent person performed it manually. For each
step, record:

| Field | Meaning |
|---|---|
| Human action | What the person decides or does |
| Inputs | Facts and documents needed |
| Systems opened | Enterprise products used to obtain facts or perform a local action |
| Output | Evidence, decision, request, or receipt produced |
| Authority | Who may perform the action |
| Stop condition | Missing or adverse evidence that prevents progress |
| Recovery | What can be reused after correction |

This table is the control against putting workflow logic inside a twin.

### 5. Research the enterprise environment

Use current primary sources:

- official API references;
- official product integration guides;
- standards bodies;
- government or regulator documentation;
- vendor webhook, error, permission, and state-machine documentation.

Record the research date and API version where the source publishes one. Use
secondary sources only to locate primary documentation, not to support contract
decisions.

For each candidate product category, extract:

- resource boundaries;
- state transitions;
- synchronous acceptance versus asynchronous completion;
- idempotency and optimistic concurrency;
- pagination and search behaviour;
- authentication, scopes, and field permissions;
- webhook delivery guarantees;
- rate limits and structured errors;
- retention and immutable-history rules;
- direct integrations with other enterprise systems.

Copy semantics, not vendor branding. Exact wire compatibility is a separate,
explicit scope choice.

Maintain a source map with the modelled semantic, official document title and
URL, consultation date, product or API version, and source heading where
available. Place a citation beside each non-obvious state, permission, event,
retention, or failure rule in the twin specification.

### 6. Select system boundaries

A candidate becomes a twin when most of these statements are true:

- it owns authoritative business records or performs a consequential local
  action;
- a human in the workflow would open or query it;
- it has an independent API, permission boundary, lifecycle, or failure mode;
- losing it should not make every other system unavailable;
- its behaviour needs state across calls;
- the SUT must integrate with it to complete or test the scenario.

Exclude a candidate when it is:

- workflow case state;
- step sequencing or retry policy;
- a human judgement;
- a policy conclusion rather than a policy source;
- a view owned by the SUT;
- a generic wrapper with no independent system of record;
- a speculative future component with no effect on the selected release.

Combine two candidates when they are modules of one product with one
transactional boundary, one permission model, and no useful independent
failure. Separate them when different owners, credentials, source records, or
failure timing matter to a test.

### 7. Assign data and action ownership

For every business fact and action, name one owning twin. Other systems may
hold snapshots or mirrors, but each copy records:

- source system and source ID;
- source resource version;
- effective time;
- retrieval or synchronisation time;
- freshness or expiry where applicable.

Do not use cross-twin foreign keys, shared tables, shared ORM models, or direct
database reads. A scenario manifest may contain aliases that link synthetic
records across systems.

### 8. Permit only real direct integrations

Draw the topology. For every twin-to-twin connection, state:

- producer and consumer;
- payload or source reference;
- transport and delivery guarantee;
- local purpose;
- behaviour it must not perform.

Direct connections are appropriate for transport, provider callbacks, and
source-record synchronisation. A direct connection must not:

- choose the next workflow step;
- combine approvals;
- request missing information on behalf of the SUT;
- reinterpret policy;
- close a workflow case;
- hide a retry that the scenario intends to test.

If a real enterprise deployment varies, choose one representative topology and
state the assumption.

### 9. Specify each twin

Use the following section for every selected twin. Bracketed names are template
fields, not unresolved decisions in a completed specification.

#### [Twin name]

Purpose:

- State why a human or SUT uses the system.

Source of truth:

- List the records and actions it owns.

Out of scope:

- List neighbouring responsibilities that remain with the SUT or another twin.

Actors and permissions:

| Actor | Read | Create | Change | Forbidden |
|---|---|---|---|---|
| [Actor] | [Resources] | [Resources] | [Resources] | [Actions or fields] |

Resources and state:

| Resource | Important fields | States | Immutability or version rule |
|---|---|---|---|
| [Resource] | [Fields] | [State machine] | [Rule] |

Public API:

| Operation | Input | Result | Local validation | Main errors |
|---|---|---|---|---|
| [Method and path] | [Request] | [Response] | [Checks] | [Codes] |

Events:

| Event | Trigger | Payload references | Delivery behaviour |
|---|---|---|---|
| [Event] | [Local transition] | [IDs and version] | [Retry, order, duplicate] |

Direct integrations:

| Other system | Direction | Purpose | Forbidden behaviour |
|---|---|---|---|
| [System] | [In or out] | [Local purpose] | [Workflow action] |

Failure and recovery:

| Failure | Observable result | Committed state | Safe recovery |
|---|---|---|---|
| [Failure] | [API or event result] | [State] | [Caller action] |

Scenario data:

- List the base records and overlays that exercise ordinary, edge, permission,
  concurrency, and recovery behaviour.

Acceptance:

- State black-box assertions using only public APIs and events.

Sources:

- Cite official documentation supporting the modelled semantics.

### 10. Apply common API rules

Define a small shared platform contract. It should cover:

- API and schema version;
- bearer authentication and scopes;
- actor, correlation, causation, and request IDs;
- idempotency for consequential writes;
- If-Match or another explicit concurrency mechanism;
- opaque cursor pagination;
- UTC timestamps and integer minor-unit money;
- stable error vocabulary with retryability;
- signed, versioned event envelopes;
- health, readiness, capabilities, and OpenAPI;
- local audit records.

Keep domain payloads in the owning twin. A common envelope must not become a
common business model.

### 11. Design realistic behaviour

For every applicable capability, cover:

- successful state transitions;
- validation failure before commit;
- denial by role or field classification;
- zero and multiple search results;
- stale writes and concurrent requests;
- partial success;
- asynchronous acceptance and later failure;
- response loss after commit;
- duplicate idempotency key with same and different data;
- delayed, duplicated, or reordered events;
- rate limiting and temporary outage;
- service restart with retained state;
- source revision or permission change.

Add only failures that the system could plausibly exhibit or that a scenario
requires. Mark an omitted category as not applicable and give one sentence
explaining why. Do not add arbitrary chaos with no observable business effect.

### 12. Specify deployment, persistence, and delivery

Name every Compose service and network. State:

- which business endpoints are available to a containerised SUT;
- which business endpoints are published to the host;
- which integration endpoints are private;
- which control endpoints are private;
- how a test proves that the SUT cannot reach control;
- how host-run and containerised SUTs register webhook targets.

For every stateful service, specify:

- its database or object store;
- its credentials and ownership boundary;
- migrations and schema compatibility;
- state retained across application restart;
- readiness dependencies;
- prohibition of another twin's database access.

For event delivery, specify:

- how a business write and local outbox event commit atomically, or the
  equivalent durability mechanism;
- the source-outbox-to-relay-to-target path;
- subscription creation, listing, deletion, and secret handling;
- event signing and acknowledgement;
- durable attempt, response, retry, and next-attempt records;
- reconciliation by reading the source resource;
- tests for duplicate, delayed, reordered, suppressed, and failed delivery.

For the control plane, specify its API and operator CLI, authentication,
network exposure, current scenario and epoch status, clock ownership, and
fault-rule records. Do not mount a Docker socket inside the conformance
container. Use a host wrapper for real service-restart tests.

### 13. Design scenarios and reset

Define:

- a versioned base seed;
- deterministic generated data;
- stable cross-system aliases;
- small scenario overlays;
- a suite-owned virtual clock;
- a private control network;
- an atomic reset protocol;
- a scenario and schema compatibility check;
- a manifest checksum;
- explicit fault rules.

Reset must restore state while containers remain running. It clears business
records, idempotency entries, outboxes, pending deliveries, caches, timers, and
faults. The estate reports ready only when every service has committed the same
scenario epoch.

Describe prepare, load, verify, commit, and abort phases. A failed reset must
leave the estate unhealthy rather than reporting a partial scenario as ready.
Fault rules must state their request match, occurrence, phase, effect,
activation count, and audit record.

Keep expected business outcomes in tests, not public twin APIs.

### 14. Map every scenario test

Create this matrix:

| Scenario case | Seeded facts | Twin behaviour under test | SUT decision under test | Observable oracle |
|---|---|---|---|---|
| [Case] | [Facts] | [Local behaviour] | [Workflow behaviour] | [Public result] |

Every original case must appear. If a case tests only the SUT, the estate must
still provide stable evidence and an observable prohibited action.

Add an External SUT exercise contract when the scenario is intended for
end-to-end use. For each case, state the SUT's public observable outcome, the
supporting twin observable, and the fact that this is SUT acceptance rather
than behaviour to add to a twin. Keep estate acceptance independently runnable
without an SUT.

### 15. Require a reference driver

Specify a small black-box client that manually calls the APIs in the correct
sequence. It proves that the twins permit a valid workflow without embedding
that workflow in a service.

Require three modes:

- happy-path, which completes one valid scenario;
- failure-modes, which asserts local denial, ambiguity, concurrency,
  idempotency, uncertain outcomes, event faults, and recovery;
- scenario-evidence, which verifies that every scenario contains the required
  facts and observability.

The driver:

- uses a small host wrapper to invoke a Docker Compose test-profile container
  and to perform real container-restart checks;
- uses public APIs for business actions;
- uses the private control API only for reset, time, faults, and diagnostics;
- never mounts the Docker socket inside the test container;
- never reads service databases;
- records an ordered request, response, event, fault, and assertion transcript;
- contains explicit scenario steps rather than a reusable orchestration engine.

Each failure assertion checks both the public error or uncertain response and
the unchanged or reconciled state visible through public APIs.

### 16. Check future extension pressure

Choose one materially different future workflow and map its probable systems.
Use it to inspect:

- entity-reference shape;
- evidence and document versioning;
- role and data-classification conventions;
- asynchronous action receipts;
- reset and scenario contracts;
- event metadata;
- archive payloads.

Keep these platform concepts domain-neutral. Do not add future services,
workflow fields, or generic engines to the first release.

### 17. Compare implementation shapes

Present two or three viable shapes before fixing the design. Usually compare:

- federated stateful services;
- a modular application with separated APIs and stores;
- a declarative simulation engine.

Compare boundary enforcement, failure realism, local operating cost, extension
cost, and risk of becoming a scripted mock. Recommend one and obtain approval.

### 18. Produce build slices and acceptance

Split implementation into independently testable slices:

1. Platform contracts and two proving services.
2. Read-only evidence sources.
3. Consequential and asynchronous actions.
4. Reset, faults, time, and observability.
5. Reference driver and scenario catalogue.
6. Documentation and future guide updates.

For each slice, state one black-box proof. End with release acceptance criteria
that can be run from a clean checkout.

## Required specification structure

The final specification contains, in this order:

1. Purpose and source scenario.
2. Decisions and release boundary.
3. Goals and exclusions.
4. Authority boundary.
5. Human workflow separated from systems.
6. System topology and permitted direct integrations.
7. Data ownership and duplicate-record rules.
8. Public API, identity, error, pagination, and event conventions.
9. One complete specification per twin.
10. Docker deployment, persistence, event delivery, and control plane.
11. Reset, time, fault, and observability design.
12. Synthetic data and scenario catalogue.
13. Reference driver.
14. Original-test coverage matrix.
15. External SUT exercise contract, when end-to-end use is intended.
16. Build slices.
17. Estate acceptance criteria.
18. Future-work pressure test.
19. Risks and controls.
20. Source map and dated primary sources.

Use tables or diagrams where they make topology, ownership, sequence, or
comparison easier to inspect.

Save the result as:

~~~text
docs/superpowers/specs/YYYY-MM-DD-[scenario-name]-digital-twins-design.md
~~~

Use a short lower-case scenario name separated by hyphens. Link the source
scenario and this guide from the saved specification.

## Review checklist

Before presenting the specification, answer every item:

### Workflow boundary

- Does any twin decide the business outcome?
- Does any twin choose the next step or manage a human task?
- Does a payment or action twin grant authority rather than enforce local
  credentials and state?
- Does a policy source evaluate a live case?
- Does an archive decide that workflow evidence is sufficient?

Any yes requires a boundary correction unless the scenario explicitly defines
that behaviour as the real external system's responsibility.

### System boundaries

- Does each fact have one owner?
- Are duplicated records identified as snapshots or mirrors?
- Are independent permissions and failures kept separate?
- Are direct integrations limited and named?
- Is shared code free of domain rules?
- Can each twin change internally without changing consumers?

### Behaviour

- Are accepted and completed states separated where the real system is
  asynchronous?
- Is idempotency durable across restart?
- Can searches expose absence and ambiguity?
- Are stale writes rejected explicitly?
- Are permissions visible as denial rather than false absence?
- Can response loss after commit be reconciled?
- Are webhook duplicate and order assumptions explicit?

### Test control

- Can the estate reset without rebuild or restart?
- Is reset atomic across services?
- Are scenario IDs, versions, epochs, and checksums visible?
- Is business time deterministic?
- Can faults target a phase and occurrence?
- Is the control plane unavailable to the SUT?

### Proof

- Does a manual public-API sequence finish the happy path?
- Do failure tests assert both returned errors and unchanged state?
- Does every original scenario case appear in the coverage matrix?
- Are SUT-only decisions labelled as such?
- Do tests avoid databases and internal object storage?
- Are request, event, fault, and assertion transcripts retained?

### Writing and sources

- Are there unresolved placeholders outside the marked template?
- Do resource states and API operations agree across sections?
- Are all named products supported by an official source?
- Is the research date stated?
- Are exclusions and deferred work explicit?
- Could an implementer derive acceptance tests without guessing?

## Patterns to reject

Reject a design that uses:

- one generic endpoint that mutates arbitrary system state;
- response fixtures with no durable records;
- a shared database that lets services read each other's tables;
- reset by destroying and rebuilding the entire estate;
- sleeps against wall time in acceptance tests;
- random failure without a recorded rule and seed;
- automatic perfect matching of customers, orders, suppliers, or invoices;
- a webhook treated as proof without source reconciliation;
- a universal approval or workflow service hidden among the twins;
- tests that pass because a twin silently fixes unsafe SUT behaviour;
- future-domain fields added to current resources without a current use.

## Output status

Mark the written specification as ready for review. Ask the user to review the
saved files before writing an implementation plan. Update the guide when a
future specification exposes a missing reusable instruction.
