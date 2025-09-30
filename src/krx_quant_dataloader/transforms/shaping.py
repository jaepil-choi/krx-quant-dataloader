"""
Shaping utilities

What this module does:
- Provides helpers to normalize column names, coerce numeric types safely, and
  reshape between wide/long formats for user-facing outputs.

Interaction:
- Used by apis/dataloader.py to produce tidy outputs when requested (opt-in).
- Does not perform hidden transforms; operations are explicit and parameterized.
"""


