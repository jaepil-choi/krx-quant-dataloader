"""
Error taxonomy (domain)

Defines typed exceptions raised and propagated across layers.

- ConfigError: issues loading or validating configuration
- RegistryValidationError: endpoint registry (YAML) is malformed
- ParamValidationError: user-supplied parameters missing/invalid
- TransportError: network or HTTP errors (already implemented in transport)
- ExtractionError: response extraction failed (already implemented in orchestration)
"""

class ConfigError(ValueError):
    """Configuration loading/validation error."""


class RegistryValidationError(ValueError):
    """Endpoint registry specification error."""


class ParamValidationError(ValueError):
    """User parameter validation error at the RawClient boundary."""


