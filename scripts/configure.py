"""Interactive configuration helper for HYDRA.

Copies ``.env.example`` to ``.env`` (preserving comments) and prompts for
the values that most deployments need to customize: container passwords,
storage DSNs, Grafana admin password, and Alertmanager webhook URLs.
Optionally substitutes the webhook URLs into
``alertmanager/alertmanager.yml``.

Usage
-----
From the repo root:

    python scripts/configure.py                 # interactive
    python scripts/configure.py --non-interactive --defaults
    python scripts/configure.py --help

Design notes
------------
* The script is stdlib-only (no third-party deps) so it runs before the
  project's own venv exists.
* Secrets are prompted via ``getpass.getpass`` so they don't end up in
  shell history or echo to the terminal.
* An existing ``.env`` is NOT overwritten unless ``--force`` is passed.
* When ``alertmanager/alertmanager.yml`` contains the
  ``<SET_IN_ENVIRONMENT:SLACK_WEBHOOK_URL>`` / ``PAGERDUTY_ROUTING_KEY``
  placeholders, the script offers to substitute real values in place —
  otherwise Alertmanager rejects the config on startup. A backup is
  written to ``alertmanager/alertmanager.yml.bak`` before edits.
* ``.env`` is verified to be gitignored before writing, so a fumbled
  secret cannot be committed.
"""

from __future__ import annotations

import argparse
import getpass
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_EXAMPLE = REPO_ROOT / ".env.example"
ENV_FILE = REPO_ROOT / ".env"
GITIGNORE = REPO_ROOT / ".gitignore"
ALERTMANAGER_YML = REPO_ROOT / "alertmanager" / "alertmanager.yml"


# Windows consoles default to cp1252 which can't encode arbitrary Unicode.
# Reconfigure stdout/stderr to UTF-8 early so comments containing em-dashes
# (and our optional glyphs) don't crash with ``UnicodeEncodeError``.
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError, OSError):
            pass


# =============================================================================
# Terminal output helpers
# =============================================================================

_USE_COLOR = sys.stdout.isatty() and os.environ.get("NO_COLOR") is None


def _can_encode(s: str) -> bool:
    """Return True if ``s`` can be written to stdout without raising.

    The Windows console defaults to cp1252 which can't encode the fancy
    Unicode prefixes we'd otherwise use. Probe the codec so we can fall
    back to ASCII glyphs without crashing on the first ``print``.
    """
    enc = getattr(sys.stdout, "encoding", None) or "ascii"
    try:
        s.encode(enc)
        return True
    except (UnicodeEncodeError, LookupError):
        return False


if _can_encode("ℹ✓⚠✗"):
    _GLYPH_INFO = "ℹ "
    _GLYPH_OK = "✓ "
    _GLYPH_WARN = "⚠ "
    _GLYPH_FAIL = "✗ "
else:
    _GLYPH_INFO = "[i] "
    _GLYPH_OK = "[OK] "
    _GLYPH_WARN = "[!] "
    _GLYPH_FAIL = "[x] "


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text


def info(msg: str) -> None:
    print(_c("36", _GLYPH_INFO) + msg)


def ok(msg: str) -> None:
    print(_c("32", _GLYPH_OK) + msg)


def warn(msg: str) -> None:
    print(_c("33", _GLYPH_WARN) + msg)


def fail(msg: str) -> None:
    print(_c("31", _GLYPH_FAIL) + msg, file=sys.stderr)


def section(title: str) -> None:
    line = "-" * max(2, 60 - len(title))
    bullet = "==" if not _can_encode("──") else "──"
    print()
    print(_c("1;34", f"{bullet} {title} {line}"))


# =============================================================================
# Prompt helpers
# =============================================================================


def ask(
    prompt: str,
    *,
    default: str | None = None,
    secret: bool = False,
    allow_empty: bool = False,
    validator: Callable[[str], bool] | None = None,
    validator_msg: str = "invalid value",
) -> str:
    """Prompt the user, returning their answer (or ``default`` on blank).

    If ``non_interactive`` mode is active (handled by caller skipping
    prompts), this is never called.
    """
    label = prompt
    if default is not None:
        label = f"{prompt} [{default if not secret else '***'}]"
    while True:
        try:
            raw = getpass.getpass(label + ": ") if secret else input(label + ": ")
        except EOFError:
            raw = ""
        except KeyboardInterrupt:
            print()
            fail("cancelled")
            sys.exit(130)

        value = raw.strip()
        if not value and default is not None:
            value = default
        if not value and not allow_empty:
            warn("value required")
            continue
        if validator is not None and value and not validator(value):
            warn(validator_msg)
            continue
        return value


def confirm(prompt: str, *, default: bool = True) -> bool:
    hint = "Y/n" if default else "y/N"
    while True:
        try:
            raw = input(f"{prompt} [{hint}]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            return default
        if not raw:
            return default
        if raw in ("y", "yes"):
            return True
        if raw in ("n", "no"):
            return False
        warn("please answer y or n")


# =============================================================================
# Validators
# =============================================================================

_SLACK_RE = re.compile(r"^https://hooks\.slack\.com/services/\S+$")
# PagerDuty Events API v2 routing keys are 32-char alphanumeric (v2 integration key)
_PAGERDUTY_RE = re.compile(r"^[A-Za-z0-9]{20,64}$")


def _valid_slack(url: str) -> bool:
    return bool(_SLACK_RE.match(url))


def _valid_pagerduty(key: str) -> bool:
    return bool(_PAGERDUTY_RE.match(key))


def _valid_nonblank(s: str) -> bool:
    return len(s) > 0


# =============================================================================
# Variable definitions — the canonical list of what we prompt for
# =============================================================================


@dataclass
class EnvVar:
    """Declarative description of a single env var the script manages."""

    name: str
    prompt: str
    default: str | None = None
    secret: bool = False
    interactive: bool = True
    allow_empty: bool = False
    validator: Callable[[str], bool] | None = None
    validator_msg: str = "invalid"
    # Section grouping for the --summary output
    section: str = "General"


# Order matters — this is the order the interactive wizard asks questions in.
VARIABLES: list[EnvVar] = [
    # --- Storage container credentials ---
    EnvVar("POSTGRES_DB", "PostgreSQL database name", default="hydra", section="PostgreSQL"),
    EnvVar("POSTGRES_USER", "PostgreSQL username", default="hydra", section="PostgreSQL"),
    EnvVar(
        "POSTGRES_PASSWORD",
        "PostgreSQL password",
        default="hydra",
        secret=True,
        validator=_valid_nonblank,
        validator_msg="password cannot be empty",
        section="PostgreSQL",
    ),
    EnvVar("NEO4J_USER", "Neo4j username", default="neo4j", section="Neo4j"),
    EnvVar(
        "NEO4J_PASSWORD",
        "Neo4j password",
        default="hydrapass",
        secret=True,
        validator=lambda s: len(s) >= 8,
        validator_msg="Neo4j requires at least 8 characters",
        section="Neo4j",
    ),
    EnvVar("MINIO_ROOT_USER", "MinIO root user", default="hydra", section="MinIO"),
    EnvVar(
        "MINIO_ROOT_PASSWORD",
        "MinIO root password",
        default="hydrapass",
        secret=True,
        validator=lambda s: len(s) >= 8,
        validator_msg="MinIO requires at least 8 characters",
        section="MinIO",
    ),
    # --- Grafana ---
    EnvVar(
        "GRAFANA_ADMIN_PASSWORD",
        "Grafana admin password",
        default="admin",
        secret=True,
        validator=_valid_nonblank,
        section="Grafana",
    ),
    # --- Alertmanager routing (optional) ---
    EnvVar(
        "SLACK_WEBHOOK_URL",
        "Slack webhook URL (blank to skip)",
        default="",
        allow_empty=True,
        validator=lambda s: not s or _valid_slack(s),
        validator_msg="must start with https://hooks.slack.com/services/",
        section="Alerts",
    ),
    EnvVar(
        "PAGERDUTY_ROUTING_KEY",
        "PagerDuty routing key (blank to skip)",
        default="",
        secret=True,
        allow_empty=True,
        validator=lambda s: not s or _valid_pagerduty(s),
        validator_msg="PagerDuty keys are 20-64 alphanumeric characters",
        section="Alerts",
    ),
    # --- HYDRA storage DSNs — computed from the container creds above ---
    # These are NOT prompted for by default; if the user accepts all
    # defaults, we compose them. A single "customize DSNs?" prompt at the
    # end lets power users override.
]


# DSN composition — parameterized by the container creds collected above.
def _compose_dsns(values: dict[str, str], *, use_localhost: bool) -> dict[str, str]:
    """Return the HYDRA_DATABASE__* values derived from container creds.

    When ``use_localhost`` is True, use localhost instead of the compose
    service hostnames — for running HYDRA directly on the host while
    the storage services are in compose.
    """
    pg_host = "localhost" if use_localhost else "postgres"
    influx_host = "localhost" if use_localhost else "influxdb"
    es_host = "localhost" if use_localhost else "elasticsearch"
    neo4j_host = "localhost" if use_localhost else "neo4j"
    minio_host = "localhost" if use_localhost else "minio"
    redis_host = "localhost" if use_localhost else "redis"

    pg_user = values.get("POSTGRES_USER", "hydra")
    pg_pass = values.get("POSTGRES_PASSWORD", "hydra")
    pg_db = values.get("POSTGRES_DB", "hydra")

    return {
        "HYDRA_DATABASE__POSTGRES_DSN": (
            f"postgresql+asyncpg://{pg_user}:{pg_pass}@{pg_host}:5432/{pg_db}"
        ),
        "HYDRA_DATABASE__INFLUXDB_URL": f"http://{influx_host}:8086",
        "HYDRA_DATABASE__ELASTICSEARCH_URL": f"http://{es_host}:9200",
        "HYDRA_DATABASE__NEO4J_URI": f"bolt://{neo4j_host}:7687",
        "HYDRA_DATABASE__MINIO_URL": f"http://{minio_host}:9000",
        "HYDRA_DATABASE__REDIS_URL": f"redis://{redis_host}:6379/0",
    }


# =============================================================================
# .env file I/O
# =============================================================================


def _load_env_example() -> list[str]:
    if not ENV_EXAMPLE.exists():
        raise FileNotFoundError(
            f".env.example not found at {ENV_EXAMPLE}. Cannot template .env."
        )
    return ENV_EXAMPLE.read_text(encoding="utf-8").splitlines()


def _apply_overrides(lines: list[str], values: dict[str, str]) -> list[str]:
    """Return ``lines`` with each ``KEY=...`` replaced by ``KEY=values[KEY]``.

    Only lines matching ``^KEY=`` (no leading ``#``) are overridden; comments
    and commented-out examples are preserved verbatim. Unknown keys in
    ``values`` are appended at the end of the file.
    """
    out: list[str] = []
    seen: set[str] = set()
    for line in lines:
        m = re.match(r"^([A-Z][A-Z0-9_]*)=", line)
        if m and m.group(1) in values:
            key = m.group(1)
            out.append(f"{key}={values[key]}")
            seen.add(key)
        else:
            out.append(line)

    extras = {k: v for k, v in values.items() if k not in seen}
    if extras:
        out.append("")
        out.append("# ----- appended by scripts/configure.py -----")
        for key, value in extras.items():
            out.append(f"{key}={value}")
    return out


def _check_gitignore() -> bool:
    """Return True if .env is covered by .gitignore."""
    if not GITIGNORE.exists():
        return False
    patterns = [p.strip() for p in GITIGNORE.read_text(encoding="utf-8").splitlines()]
    return ".env" in patterns or any(p == ".env" or p == "/.env" for p in patterns)


# =============================================================================
# Alertmanager config substitution
# =============================================================================


_ALERTMANAGER_PLACEHOLDERS = {
    "<SET_IN_ENVIRONMENT:SLACK_WEBHOOK_URL>": "SLACK_WEBHOOK_URL",
    "<SET_IN_ENVIRONMENT:PAGERDUTY_ROUTING_KEY>": "PAGERDUTY_ROUTING_KEY",
}


def _substitute_alertmanager(values: dict[str, str]) -> bool:
    """Replace placeholders in ``alertmanager.yml`` with real values.

    Returns True if the file was modified. A ``.bak`` backup is written
    alongside the original before any edit.
    """
    if not ALERTMANAGER_YML.exists():
        return False

    text = ALERTMANAGER_YML.read_text(encoding="utf-8")
    updated = text
    changed_keys: list[str] = []
    for placeholder, key in _ALERTMANAGER_PLACEHOLDERS.items():
        val = values.get(key, "")
        if not val:
            # leave the placeholder in place if the user didn't supply a value
            continue
        if placeholder in updated:
            updated = updated.replace(placeholder, val)
            changed_keys.append(key)

    if updated == text:
        return False

    backup = ALERTMANAGER_YML.with_suffix(ALERTMANAGER_YML.suffix + ".bak")
    backup.write_text(text, encoding="utf-8")
    ALERTMANAGER_YML.write_text(updated, encoding="utf-8")
    info(f"alertmanager.yml: substituted {', '.join(changed_keys)} (backup -> {backup.name})")
    return True


# =============================================================================
# Wizard
# =============================================================================


def _collect_values(args: argparse.Namespace) -> dict[str, str]:
    values: dict[str, str] = {}

    if args.non_interactive:
        # Non-interactive: accept defaults where provided, error on
        # variables with no default.
        missing: list[str] = []
        for var in VARIABLES:
            if var.default is not None:
                values[var.name] = var.default
            elif var.allow_empty:
                values[var.name] = ""
            else:
                missing.append(var.name)
        if missing:
            fail(f"--non-interactive: no default for {', '.join(missing)}")
            sys.exit(2)
        # Compose DSNs from defaults.
        values.update(_compose_dsns(values, use_localhost=args.localhost))
        return values

    # Interactive.
    current_section = None
    for var in VARIABLES:
        if var.section != current_section:
            section(var.section)
            current_section = var.section
        values[var.name] = ask(
            var.prompt,
            default=var.default,
            secret=var.secret,
            allow_empty=var.allow_empty,
            validator=var.validator,
            validator_msg=var.validator_msg,
        )

    # DSN composition
    section("HYDRA storage DSNs")
    info("Composing HYDRA_DATABASE__* URLs from the container credentials above.")
    use_localhost = False
    if confirm(
        "Run HYDRA directly on the host (storage in compose)? This uses 'localhost' "
        "in the DSNs instead of compose service names",
        default=False,
    ):
        use_localhost = True

    dsns = _compose_dsns(values, use_localhost=use_localhost)
    for key, val in dsns.items():
        # Mask password in display
        shown = re.sub(r"://[^:@]+:[^@]+@", "://***:***@", val)
        print(f"  {key}={shown}")
    values.update(dsns)

    return values


# =============================================================================
# Main
# =============================================================================


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Interactive configuration helper for HYDRA.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--force", action="store_true", help="overwrite an existing .env without asking"
    )
    p.add_argument(
        "--non-interactive",
        action="store_true",
        help="accept all defaults, fail if any required value lacks a default",
    )
    p.add_argument(
        "--localhost",
        action="store_true",
        help="(with --non-interactive) compose DSNs against localhost rather than "
        "docker-compose service names",
    )
    p.add_argument(
        "--skip-alertmanager",
        action="store_true",
        help="do not edit alertmanager/alertmanager.yml",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="print what would be written without touching any files",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    print(_c("1", "HYDRA configuration helper"))
    info(f"repo root: {REPO_ROOT}")

    # Pre-flight: .env.example must exist, .gitignore must cover .env
    try:
        template_lines = _load_env_example()
    except FileNotFoundError as e:
        fail(str(e))
        return 1

    if not _check_gitignore():
        warn(
            ".env does not appear in .gitignore — aborting to prevent accidental "
            "secret commit. Add '.env' to .gitignore and re-run."
        )
        return 1

    if ENV_FILE.exists() and not args.force and not args.dry_run:
        if not confirm(
            f".env already exists at {ENV_FILE.relative_to(REPO_ROOT)}. Overwrite?",
            default=False,
        ):
            info("aborted — existing .env kept")
            return 0

    values = _collect_values(args)

    # Render .env
    rendered = _apply_overrides(template_lines, values)
    text = "\n".join(rendered) + "\n"

    if args.dry_run:
        section("dry-run — would write")
        print(text)
        return 0

    ENV_FILE.write_text(text, encoding="utf-8")
    # Lock down permissions so other OS users can't read the secrets.
    try:
        os.chmod(ENV_FILE, 0o600)
    except (OSError, NotImplementedError):
        # On Windows without chmod semantics, fall back silently.
        pass
    ok(f"wrote {ENV_FILE.relative_to(REPO_ROOT)}")

    if not args.skip_alertmanager:
        _substitute_alertmanager(values)

    section("next steps")
    print("  1. Review .env (secrets are plain text)")
    print("  2. docker compose up -d")
    print("  3. Visit:")
    print("       API       http://localhost:8000/api/v1/health")
    print("       Prometheus http://localhost:9090")
    print("       Grafana   http://localhost:3000  (user: admin)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
