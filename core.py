"""
core.py (facade)

This module keeps the public API stable for the rest of the app, while
moving the heavy implementation into Streamlit-free modules.

- core_backend.py: DB + parsing + ingestion + computations (no Streamlit import)
- core_shared.py: Streamlit-cached shared DB connection (get_db)

All existing imports like `from core import *` continue to work.
"""

from core_backend import *  # noqa: F401,F403
from core_shared import get_db  # noqa: F401
