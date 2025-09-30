"""
High-level loaders (composition of snapshots + transforms)

What this module does:
- Exposes programmatic entry points for common data retrieval tasks that require
  more than a single raw call, e.g., fetching MDCSTAT01602 daily snapshots for
  a range and computing adj_factors.

Interaction:
- Called by apis/dataloader.py methods to implement user-facing flows while
  keeping apis thin and focused on parameter mapping.
"""


