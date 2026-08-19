import hashlib
import hmac


def digest_secret(client_id: str, secret: str, pepper: str) -> str:
    salt = hashlib.sha256(f"{pepper}:{client_id}".encode()).digest()[:16]
    value = hashlib.scrypt(secret.encode(), salt=salt, n=2**14, r=8, p=1, dklen=32)
    return value.hex()


def secret_matches(client_id: str, supplied: str, pepper: str, expected: str) -> bool:
    return hmac.compare_digest(digest_secret(client_id, supplied, pepper), expected)
