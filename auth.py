"""
auth.py
~~~~~~~
Autentisering for emneevalueringer.

Bruker de eksisterende users/sessions/roles-tabellene i AOL-databasen
(delt med AACSB-systemet).
"""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta
from http.cookies import SimpleCookie

import bcrypt
from sqlalchemy import text

from evaluation_db_mysql import _get_engine

REQUIRED_ROLE = "emneevaluering"
SESSION_MAX_AGE_DAYS = 7


def verify_session(db_name: str, token: str) -> dict | None:
    """Valider en sesjonstoken. Returnerer brukerinfo eller None."""
    engine = _get_engine(db_name)
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT u.uuid, u.email, u.firstname, u.lastname, u.active "
                "FROM sessions s "
                "JOIN users u ON u.uuid = s.user_id "
                "WHERE s.token = :token AND s.expires_at > NOW()"
            ),
            {"token": token},
        ).first()
        if not row:
            return None
        m = row._mapping
        if not m["active"]:
            return None
        return {
            "uuid": m["uuid"],
            "email": m["email"],
            "firstname": m["firstname"],
            "lastname": m["lastname"],
        }


def has_eval_access(db_name: str, user_uuid: int) -> bool:
    """Sjekk om bruker har 'emneevaluering'-rolle eller er system_admin."""
    engine = _get_engine(db_name)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT r.role_name, r.root "
                "FROM user_roles ur "
                "JOIN roles r ON r.role_id = ur.role_id "
                "WHERE ur.uuid = :uuid "
                "AND (ur.expires IS NULL OR ur.expires > NOW())"
            ),
            {"uuid": user_uuid},
        )
        for row in rows:
            m = row._mapping
            if m["root"] or m["role_name"] == REQUIRED_ROLE:
                return True
        return False


def login_user(
    db_name: str,
    email: str,
    password: str,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> str | None:
    """Valider innlogging og opprett sesjon. Returnerer sesjonstoken eller None."""
    engine = _get_engine(db_name)
    with engine.begin() as conn:
        row = conn.execute(
            text(
                "SELECT uuid, password_hash, active FROM users WHERE email = :email"
            ),
            {"email": email},
        ).first()
        if not row:
            return None
        m = row._mapping
        if not m["active"] or not m["password_hash"]:
            return None
        if not bcrypt.checkpw(
            password.encode("utf-8"), m["password_hash"].encode("utf-8")
        ):
            return None

        token = secrets.token_urlsafe(32)
        expires = datetime.utcnow() + timedelta(days=SESSION_MAX_AGE_DAYS)
        conn.execute(
            text(
                "INSERT INTO sessions (user_id, token, expires_at, ip_address, user_agent) "
                "VALUES (:user_id, :token, :expires_at, :ip_address, :user_agent)"
            ),
            {
                "user_id": m["uuid"],
                "token": token,
                "expires_at": expires,
                "ip_address": ip_address,
                "user_agent": user_agent,
            },
        )
        conn.execute(
            text("UPDATE users SET last_login = NOW() WHERE uuid = :uuid"),
            {"uuid": m["uuid"]},
        )
        return token


def logout_user(db_name: str, token: str) -> None:
    """Slett en sesjon."""
    engine = _get_engine(db_name)
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM sessions WHERE token = :token"),
            {"token": token},
        )


def parse_cookie_token(cookie_header: str | None) -> str | None:
    """Hent session_token fra en Cookie-header."""
    if not cookie_header:
        return None
    cookie = SimpleCookie()
    cookie.load(cookie_header)
    morsel = cookie.get("session_token")
    return morsel.value if morsel else None
