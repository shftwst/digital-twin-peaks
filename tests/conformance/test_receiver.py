import hashlib
import hmac
import json

from fastapi.testclient import TestClient

from enterprise_twins.conformance.receiver import create_receiver_app


def signed_headers(secret: str, event_id: str, body: bytes) -> dict[str, str]:
    timestamp = "2026-08-19T10:00:00Z"
    digest = hmac.new(
        secret.encode(),
        timestamp.encode() + b"." + body,
        hashlib.sha256,
    ).hexdigest()
    return {
        "Content-Type": "application/json",
        "X-Twin-Event-Id": event_id,
        "X-Twin-Timestamp": timestamp,
        "X-Twin-Signature": f"v1={digest}",
    }


def event_body(
    event_id: str,
    *,
    source: str = "identity",
    event_type: str = "identity.token.issued",
) -> bytes:
    return json.dumps(
        {
            "eventId": event_id,
            "eventType": event_type,
            "schemaVersion": "1.0",
            "source": source,
            "subject": "token/tok_test",
            "resourceVersion": 1,
            "correlationId": "case-retry-proof",
            "causationId": "req_test",
            "occurredAt": "2026-08-19T10:00:00Z",
            "recordedAt": "2026-08-19T10:00:00Z",
            "data": {"tokenId": "tok_test"},
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def test_receiver_records_unarmed_attempt_then_accepts_the_same_signed_event() -> None:
    client = TestClient(create_receiver_app("receiver-token"))
    event_id = "evt_retry"
    body = event_body(event_id)
    headers = signed_headers("subscription-secret", event_id, body)

    assert client.get("/health/ready").status_code == 200
    unarmed = client.post("/events", content=body, headers=headers)
    assert unarmed.status_code == 503
    assert unarmed.json()["error"]["retryable"] is True
    assert (
        client.get(
            "/internal/v1/events",
            headers={"Authorization": "Bearer receiver-token"},
        ).json()
        == []
    )

    configured = client.post(
        "/internal/v1/secrets",
        headers={"Authorization": "Bearer receiver-token"},
        json={
            "source": "identity",
            "eventType": "identity.token.issued",
            "secret": "subscription-secret",
        },
    )
    assert configured.status_code == 204
    accepted = client.post("/events", content=body, headers=headers)
    assert accepted.status_code == 204

    attempts = client.get(
        "/internal/v1/attempts",
        headers={"Authorization": "Bearer receiver-token"},
    ).json()
    events = client.get(
        "/internal/v1/events",
        headers={"Authorization": "Bearer receiver-token"},
    ).json()
    assert [item["outcome"] for item in attempts] == ["unarmed", "accepted"]
    assert [item["eventId"] for item in attempts] == [event_id, event_id]
    assert attempts[0]["bodyHash"] == attempts[1]["bodyHash"]
    assert len(events) == 1
    assert events[0]["eventId"] == event_id
    assert events[0]["signatureValid"] is True


def test_receiver_rejects_bad_credentials_signature_and_event_id_without_acceptance() -> None:
    client = TestClient(create_receiver_app("receiver-token"))
    private = client.get(
        "/internal/v1/events",
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert private.status_code == 401
    assert private.json()["error"]["code"] == "unauthenticated"

    configured = client.post(
        "/internal/v1/secrets",
        headers={"Authorization": "Bearer receiver-token"},
        json={
            "source": "identity",
            "eventType": "identity.token.issued",
            "secret": "subscription-secret",
        },
    )
    assert configured.status_code == 204

    body = event_body("evt_expected")
    wrong_signature = signed_headers("wrong-secret", "evt_expected", body)
    rejected = client.post("/events", content=body, headers=wrong_signature)
    assert rejected.status_code == 401
    assert rejected.json()["error"]["code"] == "unauthenticated"

    mismatched = client.post(
        "/events",
        content=body,
        headers=signed_headers("subscription-secret", "evt_other", body),
    )
    assert mismatched.status_code == 422
    events = client.get(
        "/internal/v1/events",
        headers={"Authorization": "Bearer receiver-token"},
    ).json()
    assert events == []


def test_receiver_rejects_valid_signature_from_a_different_source_and_event_type() -> None:
    client = TestClient(create_receiver_app("receiver-token"))
    for source, event_type, secret in (
        ("identity", "identity.token.issued", "identity-secret"),
        ("crm", "crm.note.created", "crm-secret"),
    ):
        configured = client.post(
            "/internal/v1/secrets",
            headers={"Authorization": "Bearer receiver-token"},
            json={"source": source, "eventType": event_type, "secret": secret},
        )
        assert configured.status_code == 204

    event_id = "evt_cross_subscription"
    body = event_body(event_id, source="crm", event_type="crm.note.created")
    rejected = client.post(
        "/events",
        content=body,
        headers=signed_headers("identity-secret", event_id, body),
    )

    assert rejected.status_code == 401
    assert rejected.json()["error"]["code"] == "unauthenticated"
    attempts = client.get(
        "/internal/v1/attempts",
        headers={"Authorization": "Bearer receiver-token"},
    ).json()
    events = client.get(
        "/internal/v1/events",
        headers={"Authorization": "Bearer receiver-token"},
    ).json()
    assert [item["outcome"] for item in attempts] == ["signature_rejected"]
    assert events == []
