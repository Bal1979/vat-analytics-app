"""
Brugerstyring for VAT Analytics (FastAPI-port af SAF-T/VIES/Data Extract-auth).

- Brugere gemmes i SQLite (data/auth.db) med hashede passwords (stdlib pbkdf2:sha256).
- Login via session-cookie (Starlette SessionMiddleware; kræver SECRET_KEY i produktion).
- Første kørsel: /setup opretter den første administrator. Forsvinder når der findes
  mindst én bruger.
- Admin kan oprette invitationslinks; modtageren vælger selv brugernavn/password.
- Invitationstokens gemmes kun som SHA-256-hash og er engangs + tidsbegrænsede.
- Login-rate-limit ligger i SQLite (multi-worker-sikkert), CSRF er session-baseret,
  og login er timing-sikkert (samme arbejde uanset om brugernavnet findes).

BEMÆRK (deploy): SQLite-filen ligger på lokal disk. På Railway nulstilles
filsystemet ved redeploy — sæt AUTH_DB_PATH til et persistent volumen
(fx /data/auth.db), ellers forsvinder admin-kontoen ved hver deploy.

Ingen credentials findes i kode eller miljøvariabler: den eneste vej ind er
/setup (første gang) og invitationslinks.
"""

import base64
import hashlib
import hmac
import logging
import os
import secrets
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse, Response
from fastapi.templating import Jinja2Templates

import audit_log

logger = logging.getLogger(__name__)

INVITE_TTL_DAYS = 7
MIN_PASSWORD_LENGTH = 10
SESSION_LIFETIME_HOURS = 12

# Login-rate-limit. Tællerne ligger i SQLite (tabellen login_attempts) og IKKE
# i RAM — flere worker-processer ville ellers kunne omgå en in-memory-tæller.
# Grænserne gælder FEJLEDE forsøg inden for vinduet.
LOGIN_WINDOW_MINUTES = 15
LOGIN_MAX_FAILS_PER_USER = 8
LOGIN_MAX_FAILS_PER_IP = 20
LOGIN_ATTEMPT_RETENTION_HOURS = 24
LOCKOUT_MESSAGE = "For mange loginforsøg. Prøv igen om et kvarter."

_PBKDF2_ALGO = "pbkdf2_sha256"
_PBKDF2_ROUNDS = 240000

_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_DB_PATH = os.path.join(_REPO_ROOT, "data", "auth.db")
_TEMPLATES_DIR = os.path.join(_REPO_ROOT, "templates")

templates = Jinja2Templates(directory=_TEMPLATES_DIR)
router = APIRouter()


# ---------------------------------------------------------------------------
# Password-hashing (ren stdlib — ingen werkzeug/passlib-afhængighed)
# ---------------------------------------------------------------------------

def generate_password_hash(password: str) -> str:
    """pbkdf2:sha256-hash på formen 'pbkdf2_sha256$<rounds>$<salt_b64>$<hash_b64>'."""
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _PBKDF2_ROUNDS)
    return f"{_PBKDF2_ALGO}${_PBKDF2_ROUNDS}${base64.b64encode(salt).decode()}${base64.b64encode(dk).decode()}"


def check_password_hash(stored: str, password: str) -> bool:
    """Konstant-tids-verifikation mod en gemt hash. False ved ukendt format."""
    try:
        algo, rounds_s, salt_b64, hash_b64 = stored.split("$")
        if algo != _PBKDF2_ALGO:
            return False
        rounds = int(rounds_s)
        salt = base64.b64decode(salt_b64)
        expected = base64.b64decode(hash_b64)
    except (ValueError, TypeError):
        return False
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, rounds)
    return hmac.compare_digest(dk, expected)


# Forudberegnet dummy-hash til timing-sikker login (samme arbejde uanset om
# brugernavnet findes). Beregnes én gang ved import — ikke pr. login-request.
_DUMMY_PASSWORD_HASH = generate_password_hash("dummy-password-for-timing")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def _db_path() -> str:
    return os.environ.get("AUTH_DB_PATH", _DEFAULT_DB_PATH)


def _connect() -> sqlite3.Connection:
    path = _db_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # check_same_thread=False: FastAPI afvikler sync-dependencies og selve ruten i
    # forskellige threadpool-tråde inden for samme request. Forbindelsen er kortlivet
    # (én pr. request, lukkes i get_db.finally) og bruges aldrig samtidigt.
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def get_db():
    """FastAPI-dependency: kortlivet forbindelse pr. request (lukkes i finally)."""
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    path = _db_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                username      TEXT NOT NULL UNIQUE COLLATE NOCASE,
                password_hash TEXT NOT NULL,
                role          TEXT NOT NULL CHECK (role IN ('admin', 'user')),
                created_at    TEXT NOT NULL,
                last_login_at TEXT
            );

            CREATE TABLE IF NOT EXISTS invites (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                token_hash TEXT NOT NULL UNIQUE,
                role       TEXT NOT NULL CHECK (role IN ('admin', 'user')),
                note       TEXT,
                created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                used_at    TEXT,
                used_by    INTEGER REFERENCES users(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS login_attempts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                username     TEXT NOT NULL COLLATE NOCASE,
                ip           TEXT NOT NULL,
                attempted_at TEXT NOT NULL,
                success      INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_login_attempts_user
                ON login_attempts (username, attempted_at);
            CREATE INDEX IF NOT EXISTS idx_login_attempts_ip
                ON login_attempts (ip, attempted_at);
            """
        )
        conn.commit()
    finally:
        conn.close()


def _now():
    return datetime.now(timezone.utc)


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(value):
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def user_count(db) -> int:
    return db.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Rate limiting på login (multi-worker-sikkert via SQLite)
# ---------------------------------------------------------------------------

def _client_ip(request: Request) -> str:
    """Klientens IP bag Railways proxy (X-Forwarded-For, første element)."""
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        first = xff.split(",")[0].strip()
        if first:
            return first
    return request.client.host if request.client else "ukendt"


def _purge_old_login_attempts(db):
    cutoff = _iso(_now() - timedelta(hours=LOGIN_ATTEMPT_RETENTION_HOURS))
    db.execute("DELETE FROM login_attempts WHERE attempted_at < ?", (cutoff,))


def _login_lockout_reason(db, username, ip):
    """Returnér 'brugernavn'/'ip' ved for mange fejlede forsøg i vinduet, ellers None."""
    cutoff = _iso(_now() - timedelta(minutes=LOGIN_WINDOW_MINUTES))
    user_fails = db.execute(
        "SELECT COUNT(*) AS n FROM login_attempts "
        "WHERE username = ? AND success = 0 AND attempted_at >= ?",
        (username, cutoff),
    ).fetchone()["n"]
    if user_fails >= LOGIN_MAX_FAILS_PER_USER:
        return "brugernavn"
    ip_fails = db.execute(
        "SELECT COUNT(*) AS n FROM login_attempts "
        "WHERE ip = ? AND success = 0 AND attempted_at >= ?",
        (ip, cutoff),
    ).fetchone()["n"]
    if ip_fails >= LOGIN_MAX_FAILS_PER_IP:
        return "ip"
    return None


def _record_login_attempt(db, username, ip, success):
    db.execute(
        "INSERT INTO login_attempts (username, ip, attempted_at, success) "
        "VALUES (?, ?, ?, ?)",
        (username, ip, _iso(_now()), 1 if success else 0),
    )
    if success:
        db.execute(
            "DELETE FROM login_attempts WHERE username = ? AND success = 0",
            (username,),
        )


# ---------------------------------------------------------------------------
# CSRF (let, session-baseret)
# ---------------------------------------------------------------------------

def csrf_token(request: Request) -> str:
    tok = request.session.get("_csrf")
    if not tok:
        tok = secrets.token_urlsafe(32)
        request.session["_csrf"] = tok
    return tok


async def verify_csrf(request: Request):
    """Dependency: verificér CSRF-token fra X-CSRF-Token-header eller form-felt '_csrf'.

    Header tjekkes først, så fil-uploads ikke behøver at få form'en parset to gange.
    Sammenligning er konstant-tid.
    """
    expected = request.session.get("_csrf", "")
    sent = request.headers.get("X-CSRF-Token", "")
    if not sent:
        form = await request.form()
        sent = form.get("_csrf", "")
    if not expected or not hmac.compare_digest(sent, expected):
        raise HTTPException(400, "Ugyldig eller manglende CSRF-token. Genindlæs siden.")


# ---------------------------------------------------------------------------
# Aktuel bruger + adgangskontrol (dependencies)
# ---------------------------------------------------------------------------

def current_user(request: Request, db):
    uid = request.session.get("user_id")
    if uid is None:
        return None
    return db.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()


_JSON_PREFIXES = ("/analyze", "/preview", "/status", "/result")


def require_login(request: Request, db=Depends(get_db)):
    """Dependency: kræv login. API-ruter får 401 JSON, HTML-ruter redirectes."""
    if user_count(db) == 0:
        raise HTTPException(303, headers={"Location": "/setup"})
    user = current_user(request, db)
    if user is None:
        if request.url.path.startswith(_JSON_PREFIXES):
            raise HTTPException(401, "Login krævet. Genindlæs siden og log ind.")
        nxt = request.url.path
        raise HTTPException(303, headers={"Location": f"/login?next={nxt}"})
    return user


def require_admin(request: Request, db=Depends(get_db)):
    user = current_user(request, db)
    if user is None:
        raise HTTPException(303, headers={"Location": "/login"})
    if user["role"] != "admin":
        raise HTTPException(403, "Kræver administrator-rettigheder.")
    return user


# ---------------------------------------------------------------------------
# Wiring
# ---------------------------------------------------------------------------

def secret_key() -> str:
    secret = os.environ.get("SECRET_KEY")
    if not secret:
        secret = secrets.token_hex(32)
        logger.warning(
            "SECRET_KEY er ikke sat — bruger tilfældig nøgle. Alle sessions "
            "invalideres ved genstart. Sæt SECRET_KEY i produktion."
        )
    return secret


def init_auth(app):
    """Init auth-db + audit-db og kobl router på appen. SessionMiddleware tilføjes
    i main.py (skal sættes på app-objektet, før requests håndteres)."""
    init_db()
    audit_log.init_audit_db()
    app.include_router(router)


# ---------------------------------------------------------------------------
# Validering af input
# ---------------------------------------------------------------------------

def _validate_username(username):
    if not (3 <= len(username) <= 64):
        return "Brugernavn skal være 3-64 tegn."
    if not all(c.isalnum() or c in "._-@" for c in username):
        return "Brugernavn må kun indeholde bogstaver, tal og . _ - @"
    return None


def _validate_password(password, password2):
    if len(password) < MIN_PASSWORD_LENGTH:
        return f"Password skal være mindst {MIN_PASSWORD_LENGTH} tegn."
    if password != password2:
        return "De to passwords er ikke ens."
    return None


def _render(request: Request, name: str, status_code: int = 200, **ctx):
    context = {"csrf": csrf_token(request)}
    context.update(ctx)
    return templates.TemplateResponse(request, name, context, status_code=status_code)


# ---------------------------------------------------------------------------
# Routes: setup / login / logout
# ---------------------------------------------------------------------------

@router.get("/setup")
def setup_get(request: Request, db=Depends(get_db)):
    if user_count(db) > 0:
        return RedirectResponse("/login", status_code=303)
    return _render(request, "auth_setup.html", error=None)


@router.post("/setup")
async def setup_post(request: Request, db=Depends(get_db), _=Depends(verify_csrf)):
    if user_count(db) > 0:
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    username = (form.get("username") or "").strip()
    password = form.get("password") or ""
    password2 = form.get("password2") or ""
    error = _validate_username(username) or _validate_password(password, password2)
    if error is not None:
        return _render(request, "auth_setup.html", error=error)
    db.execute(
        "INSERT INTO users (username, password_hash, role, created_at) "
        "VALUES (?, ?, 'admin', ?)",
        (username, generate_password_hash(password), _iso(_now())),
    )
    db.commit()
    row = db.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
    request.session.clear()
    request.session["user_id"] = row["id"]
    audit_log.log("user.setup_admin", actor=username, ip=_client_ip(request))
    logger.info("Administrator oprettet: %s", username)
    return RedirectResponse("/", status_code=303)


@router.get("/login")
def login_get(request: Request, next: str = "", db=Depends(get_db)):
    if user_count(db) == 0:
        return RedirectResponse("/setup", status_code=303)
    if current_user(request, db) is not None:
        return RedirectResponse("/", status_code=303)
    return _render(request, "auth_login.html", error=None, next=next)


@router.post("/login")
async def login_post(request: Request, db=Depends(get_db), _=Depends(verify_csrf)):
    if user_count(db) == 0:
        return RedirectResponse("/setup", status_code=303)
    form = await request.form()
    username = (form.get("username") or "").strip()
    password = form.get("password") or ""
    next_target = form.get("next") or ""
    ip = _client_ip(request)

    _purge_old_login_attempts(db)
    lockout = _login_lockout_reason(db, username, ip)
    if lockout is not None:
        db.commit()
        logger.warning("Login-lockout (%s) for brugernavn=%r fra ip=%s", lockout, username, ip)
        audit_log.log("login.lockout", actor=username, ip=ip, outcome="blocked")
        return _render(request, "auth_login.html", error=LOCKOUT_MESSAGE, next=next_target)

    row = db.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
    # check_password_hash køres altid (også ved ukendt bruger) for at undgå
    # timing-forskel mellem 'ukendt bruger' og 'forkert password'.
    ok = check_password_hash(row["password_hash"] if row else _DUMMY_PASSWORD_HASH, password)
    _record_login_attempt(db, username, ip, success=(row is not None and ok))
    if row is not None and ok:
        request.session.clear()
        request.session["user_id"] = row["id"]
        db.execute("UPDATE users SET last_login_at = ? WHERE id = ?", (_iso(_now()), row["id"]))
        db.commit()
        audit_log.log("login.success", actor=username, ip=ip)
        target = next_target or "/"
        if not target.startswith("/") or target.startswith("//"):
            target = "/"
        return RedirectResponse(target, status_code=303)
    db.commit()
    audit_log.log("login.failure", actor=username, ip=ip, outcome="fail")
    logger.warning("Mislykket login for brugernavn: %r", username)
    return _render(request, "auth_login.html", error="Forkert brugernavn eller password.", next=next_target)


@router.post("/logout")
def logout(request: Request, db=Depends(get_db), _=Depends(verify_csrf)):
    u = current_user(request, db)
    audit_log.log("logout", actor=u["username"] if u else None, ip=_client_ip(request))
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


# ---------------------------------------------------------------------------
# Routes: admin — brugere og invitationer
# ---------------------------------------------------------------------------

@router.get("/admin/users")
def admin_users(request: Request, db=Depends(get_db), admin=Depends(require_admin)):
    users = db.execute(
        "SELECT id, username, role, created_at, last_login_at FROM users ORDER BY created_at"
    ).fetchall()
    invites = db.execute(
        "SELECT i.id, i.role, i.note, i.created_at, i.expires_at, i.used_at, "
        "       u.username AS used_by_name "
        "FROM invites i LEFT JOIN users u ON u.id = i.used_by "
        "ORDER BY i.created_at DESC LIMIT 50"
    ).fetchall()
    new_invite_link = request.session.pop("new_invite_link", None)
    return _render(
        request, "auth_admin.html",
        users=users, invites=invites, now=_iso(_now()),
        new_invite_link=new_invite_link, me=admin,
    )


@router.post("/admin/invites")
async def admin_create_invite(request: Request, db=Depends(get_db),
                              admin=Depends(require_admin), _=Depends(verify_csrf)):
    form = await request.form()
    role = form.get("role", "user")
    if role not in ("admin", "user"):
        role = "user"
    note = (form.get("note") or "").strip()[:200]
    token = secrets.token_urlsafe(32)
    db.execute(
        "INSERT INTO invites (token_hash, role, note, created_by, created_at, expires_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (_hash_token(token), role, note, admin["id"], _iso(_now()),
         _iso(_now() + timedelta(days=INVITE_TTL_DAYS))),
    )
    db.commit()
    # Linket vises én gang til admin — kun hash gemmes i databasen.
    base = str(request.base_url).rstrip("/")
    request.session["new_invite_link"] = f"{base}/invite/{token}"
    audit_log.log("user.invite_created", actor=admin["username"],
                  detail=f"rolle={role}", ip=_client_ip(request))
    logger.info("Invitation oprettet (rolle=%s)", role)
    return RedirectResponse("/admin/users", status_code=303)


@router.post("/admin/invites/{invite_id}/revoke")
def admin_revoke_invite(invite_id: int, request: Request, db=Depends(get_db),
                        admin=Depends(require_admin), _=Depends(verify_csrf)):
    db.execute("DELETE FROM invites WHERE id = ? AND used_at IS NULL", (invite_id,))
    db.commit()
    audit_log.log("user.invite_revoked", actor=admin["username"],
                  detail=f"invite_id={invite_id}", ip=_client_ip(request))
    return RedirectResponse("/admin/users", status_code=303)


@router.post("/admin/users/{user_id}/delete")
def admin_delete_user(user_id: int, request: Request, db=Depends(get_db),
                      admin=Depends(require_admin), _=Depends(verify_csrf)):
    if user_id == admin["id"]:
        raise HTTPException(400, "Du kan ikke slette din egen konto.")
    target = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    if target is None:
        raise HTTPException(404, "Bruger ikke fundet.")
    if target["role"] == "admin":
        admins = db.execute("SELECT COUNT(*) AS n FROM users WHERE role = 'admin'").fetchone()["n"]
        if admins <= 1:
            raise HTTPException(400, "Kan ikke slette den sidste administrator.")
    db.execute("DELETE FROM users WHERE id = ?", (user_id,))
    db.commit()
    audit_log.log("user.deleted", actor=admin["username"],
                  detail=f"slettet={target['username']} (rolle={target['role']})",
                  ip=_client_ip(request))
    logger.info("Bruger slettet: %s (af %s)", target["username"], admin["username"])
    return RedirectResponse("/admin/users", status_code=303)


@router.post("/admin/backup")
def admin_backup(request: Request, db=Depends(get_db),
                 admin=Depends(require_admin), _=Depends(verify_csrf)):
    """Download en konsistent sikkerhedskopi af brugerdatabasen via sqlite3's backup-API.

    BEMÆRK: Kopien indeholder password-hashes — den skal opbevares sikkert.
    """
    fd, tmp_path = tempfile.mkstemp(prefix="auth-backup-", suffix=".db")
    os.close(fd)
    try:
        dst = sqlite3.connect(tmp_path)
        try:
            db.backup(dst)
        finally:
            dst.close()
        with open(tmp_path, "rb") as fh:
            payload = fh.read()
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
    filename = "balai-brugere-%s.db" % _now().strftime("%Y%m%d-%H%M")
    audit_log.log("admin.backup_downloaded", actor=admin["username"],
                  detail=f"{len(payload)} bytes", ip=_client_ip(request))
    logger.info("Sikkerhedskopi af brugerdatabasen downloadet af %s (%d bytes)",
                admin["username"], len(payload))
    return Response(
        payload, media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"',
                 "Cache-Control": "no-store"},
    )


# ---------------------------------------------------------------------------
# Routes: accepter invitation
# ---------------------------------------------------------------------------

def _load_valid_invite(db, token):
    row = db.execute("SELECT * FROM invites WHERE token_hash = ?", (_hash_token(token),)).fetchone()
    if row is None or row["used_at"] is not None:
        return None
    if _parse_iso(row["expires_at"]) < _now():
        return None
    return row


@router.get("/invite/{token}")
def accept_invite_get(token: str, request: Request, db=Depends(get_db)):
    invite = _load_valid_invite(db, token)
    if invite is None:
        return _render(request, "auth_invite.html", status_code=410, invalid=True, note=None, error=None)
    return _render(request, "auth_invite.html", invalid=False, note=invite["note"], error=None)


@router.post("/invite/{token}")
async def accept_invite_post(token: str, request: Request, db=Depends(get_db), _=Depends(verify_csrf)):
    invite = _load_valid_invite(db, token)
    if invite is None:
        return _render(request, "auth_invite.html", status_code=410, invalid=True, note=None, error=None)
    form = await request.form()
    username = (form.get("username") or "").strip()
    password = form.get("password") or ""
    password2 = form.get("password2") or ""
    error = _validate_username(username) or _validate_password(password, password2)
    if error is None and db.execute("SELECT 1 FROM users WHERE username = ?", (username,)).fetchone():
        error = "Brugernavnet er optaget. Vælg et andet."
    if error is not None:
        return _render(request, "auth_invite.html", invalid=False, note=invite["note"], error=error)
    db.execute(
        "INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
        (username, generate_password_hash(password), invite["role"], _iso(_now())),
    )
    row = db.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
    db.execute("UPDATE invites SET used_at = ?, used_by = ? WHERE id = ?",
               (_iso(_now()), row["id"], invite["id"]))
    db.commit()
    request.session.clear()
    request.session["user_id"] = row["id"]
    audit_log.log("user.created", actor=username,
                  detail=f"rolle={invite['role']} (via invitation)", ip=_client_ip(request))
    logger.info("Invitation accepteret: %s (rolle=%s)", username, invite["role"])
    return RedirectResponse("/", status_code=303)
