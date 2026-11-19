import hashlib
import hmac
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from ton_payment.config import settings

SIGNATURE_HEADER = "X-Signature"
_bearer = HTTPBearer(auto_error=False)


async def require_token(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None,
        Depends(_bearer),
    ],
) -> None:
    expected = settings.api_token.get_secret_value().encode()
    if credentials is None or not hmac.compare_digest(
        credentials.credentials.encode(),
        expected,
    ):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )


def generate_signature(api_key: str, body: bytes) -> str:
    token_hash = hashlib.sha256(api_key.encode()).digest()
    return hmac.new(token_hash, body, hashlib.sha256).hexdigest()
