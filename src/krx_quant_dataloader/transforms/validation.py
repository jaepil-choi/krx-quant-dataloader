"""
Validation utilities (lightweight)

What this module does:
- Contains simple row-level checks for presence of critical keys (e.g., for
  MDCSTAT01602: ISU_SRT_CD, BAS_PRC, TDD_CLSPRC), raising clear errors when
  expectations are not met.

Interaction:
- Can be used by pipelines or apis to assert expected schemas before shaping.
  Keep checks minimal to preserve flexibility.
"""


