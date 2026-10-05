"""The domain package must stay pure and deterministic (plan section 17).

An allowlist is stricter than an import-linter denylist: a library nobody thought
to ban still fails here."""

import ast
import sys
from pathlib import Path

import watchtower_domain

# Stdlib modules that do I/O or read clocks and randomness. Domain logic gets
# time only from event data, so replays reproduce the live run exactly.
IMPURE_STDLIB = frozenset(
    {
        "asyncio", "http", "io", "multiprocessing", "os", "pathlib", "random", "secrets",
        "shutil", "socket", "sqlite3", "ssl", "subprocess", "threading", "time", "urllib",
    }
)  # fmt: skip
ALLOWED = (sys.stdlib_module_names - IMPURE_STDLIB) | {"watchtower_domain"}


def disallowed_imports(source: str) -> list[str]:
    found: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            found += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            found.append(node.module)
    return [name for name in found if name.split(".")[0] not in ALLOWED]


def test_domain_imports_only_pure_stdlib() -> None:
    root = Path(watchtower_domain.__file__).parent
    offenders = {
        path.name: bad
        for path in sorted(root.rglob("*.py"))
        if (bad := disallowed_imports(path.read_text(encoding="utf-8")))
    }
    assert offenders == {}


def test_guard_catches_io_clock_and_third_party_imports() -> None:
    source = "import confluent_kafka\nfrom time import monotonic\nimport os.path\nimport math"
    assert disallowed_imports(source) == ["confluent_kafka", "time", "os.path"]
