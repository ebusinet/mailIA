from datetime import datetime, timedelta, timezone
import bcrypt
from jose import jwt, JWTError
from cryptography.fernet import Fernet
from src.config import get_settings


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode(), hashed.encode())


# Tous les jetons du produit sont signes par la meme cle et le meme algorithme : seule
# la revendication `purpose` les distingue. Elle est donc exigee POSITIVEMENT — un jeton
# qui ne la porte pas est refuse. Verifier « purpose != reset » aurait suffi a fermer le
# cas connu, mais aurait laisse passer tout futur type de jeton qu'on oublierait de
# declarer : c'est encore traiter une absence de signal comme une autorisation.
PURPOSE_ACCESS = "access"
PURPOSE_RESET = "reset"


def create_access_token(data: dict, remember: bool = False) -> str:
    settings = get_settings()
    to_encode = data.copy()
    to_encode["purpose"] = PURPOSE_ACCESS
    if remember:
        expire = datetime.now(timezone.utc) + timedelta(days=30)
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=settings.access_token_expire_minutes)
    to_encode["exp"] = expire
    return jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)


def password_fingerprint(password_hash: str) -> str:
    """Empreinte courte du hash courant, pour rendre un jeton de reinitialisation
    invalide des que le mot de passe change — sans table de jti ni colonne en base."""
    import hashlib
    return hashlib.sha256((password_hash or "").encode()).hexdigest()[:16]


def create_reset_token(user_id: int, password_hash: str) -> str:
    settings = get_settings()
    expire = datetime.now(timezone.utc) + timedelta(minutes=30)
    return jwt.encode(
        {
            "sub": str(user_id),
            "purpose": PURPOSE_RESET,
            "pv": password_fingerprint(password_hash),
            "exp": expire,
        },
        settings.secret_key,
        algorithm=settings.algorithm,
    )


def decode_reset_token(token: str) -> dict | None:
    """Decode a password reset token. Returns {"user_id", "pv"} or None."""
    settings = get_settings()
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        if payload.get("purpose") != PURPOSE_RESET:
            return None
        return {"user_id": int(payload["sub"]), "pv": payload.get("pv")}
    except (JWTError, KeyError, ValueError):
        return None


def decode_access_token(token: str) -> dict | None:
    settings = get_settings()
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
    except JWTError:
        return None
    # Exigence positive : sans `purpose: access`, le jeton n'ouvre pas de session.
    # Un jeton de reinitialisation ouvrait sinon la session complete de la victime,
    # droits d'administration compris.
    if payload.get("purpose") != PURPOSE_ACCESS:
        return None
    return payload


def _get_fernet() -> Fernet:
    return Fernet(get_settings().encryption_key.encode())


def encrypt_value(value: str) -> str:
    return _get_fernet().encrypt(value.encode()).decode()


def decrypt_value(encrypted: str) -> str:
    return _get_fernet().decrypt(encrypted.encode()).decode()
