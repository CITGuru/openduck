import os
import re
from pathlib import Path
from typing import Optional


URI_RE = re.compile(r"^(?:openduck|od):(.*)$", re.IGNORECASE)

EXTENSION_SEARCH_PATHS = [
    Path(__file__).resolve().parents[4]
    / "extensions"
    / "openduck"
    / "build"
    / "release"
    / "extension"
    / "openduck"
    / "openduck.duckdb_extension",
]


def find_extension() -> Optional[str]:
    """Auto-detect the built extension binary."""
    env = os.environ.get("OPENDUCK_EXTENSION_PATH")
    if env:
        p = Path(env)
        if p.exists():
            return str(p)

    for candidate in EXTENSION_SEARCH_PATHS:
        if candidate.exists():
            return str(candidate)

    return None


def parse_uri(uri: str) -> tuple[str, dict[str, str]]:
    """Parse 'openduck:mydb?key=val&...' into (database, params)."""
    qpos = uri.find("?")
    if qpos == -1:
        path_part, query = uri, ""
    else:
        path_part, query = uri[:qpos], uri[qpos + 1 :]

    m = URI_RE.match(path_part)
    database = (m.group(1) if m else path_part) or "default"

    params: dict[str, str] = {}
    if query:
        for pair in query.split("&"):
            eq = pair.find("=")
            if eq == -1:
                params[pair] = ""
            else:
                params[pair[:eq]] = pair[eq + 1 :]

    return database, params
