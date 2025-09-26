from .config import ConfigFacade
from .adapter import AdapterRegistry
from .transport import Transport
from .orchestration import Orchestrator
from .client import RawClient
from .apis import DataLoader


def create_dataloader_from_yaml(config_path: str) -> DataLoader:
    cfg = ConfigFacade.load(config_path=config_path)
    reg = AdapterRegistry.load(config_path=config_path)
    transport = Transport(cfg)
    orch = Orchestrator(transport)
    raw = RawClient(registry=reg, orchestrator=orch)
    return DataLoader(raw_client=raw)


