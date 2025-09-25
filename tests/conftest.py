from pathlib import Path
import pytest


@pytest.fixture(scope="session")
def test_config_path() -> str:
    """Absolute path to the canonical test configuration YAML.

    Kept under tests/fixtures to ensure tests are hermetic and independent
    of documentation file layout.
    """
    return str(Path(__file__).parent / "fixtures" / "test_config.yaml")


