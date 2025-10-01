"""
High-level loaders (composition of snapshots + transforms)

Status: PLACEHOLDER (not yet implemented)

Purpose:
- Programmatic entry points for common data retrieval tasks requiring composition
  of multiple raw calls, transforms, and optional persistence.
- Example: fetch daily snapshots for a range, compute adj_factors, and return
  tidy DataFrame without requiring caller to manage writer lifecycle.

Interaction:
- Called by apis/dataloader.py methods to implement user-facing flows while
  keeping APIs thin and focused on parameter mapping.

Note:
- Current implementation delegates to pipelines/snapshots.py with explicit writer injection.
- This module can provide convenience wrappers that manage writer lifecycle internally.
"""


