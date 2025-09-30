"""
Pipelines layer

What this package does:
- Encapsulates multi-step workflows that combine raw fetching (via RawClient)
  and pure transforms. Examples:
  * snapshots: per-day strtDd=endDd=D loops for MDCSTAT01602
  * loaders: convenience routines combining fetching + adjustment + shaping

Design rules:
- No direct network code; all IO goes through RawClient.
- No implicit storage side-effects; return data to callers.
"""


