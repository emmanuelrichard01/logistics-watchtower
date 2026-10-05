from importlib.metadata import requires


def test_domain_declares_no_runtime_dependencies() -> None:
    # Stopgap for the import-linter contract (plan day 3): any new dependency
    # in the domain package must be a deliberate decision, not a drive-by.
    assert requires("watchtower-domain") is None
