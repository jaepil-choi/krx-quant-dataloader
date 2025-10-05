from pathlib import Path
import pytest


@pytest.fixture(scope="session")
def test_config_path() -> str:
    """Absolute path to the canonical test configuration YAML (endpoints.yaml format).

    Kept under tests/fixtures to ensure tests are hermetic and independent
    of documentation file layout.
    
    Note: This is endpoints.yaml format, used by AdapterRegistry tests.
    For ConfigFacade tests, use test_settings_path instead.
    """
    return str(Path(__file__).parent / "fixtures" / "test_config.yaml")


@pytest.fixture(scope="session")
def test_settings_path() -> str:
    """Absolute path to the test settings.yaml file.
    
    This is the main settings file (settings.yaml format) used by ConfigFacade tests.
    It references test_config.yaml as the endpoints file.
    """
    return str(Path(__file__).parent / "fixtures" / "test_settings.yaml")


