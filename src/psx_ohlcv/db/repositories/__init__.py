"""Repository modules for domain-specific database operations.

Re-exports all public functions from repository sub-modules so callers
can use ``from psx_ohlcv.db.repositories import <func>``.

Import order matters for 7 duplicate function names:
  - record_sync_run_start, record_sync_run_end, record_failure
      in eod + jobs → canonical home: jobs (imported last)
  - get_symbol_activity
      in symbols + user → canonical home: user (imported last)
  - upsert_yield_curve_point, get_yield_curve, get_latest_yield_curve
      in fixed_income + market → canonical home: market (imported last)
"""

# 1. symbols  (10 unique + 1 duplicate overridden later by user)
from .symbols import *  # noqa: F401, F403

# 2. eod  (16 unique + 3 duplicates overridden later by jobs)
from .eod import *  # noqa: F401, F403

# 3. intraday  (6 public functions)
from .intraday import *  # noqa: F401, F403

# 4. company  (29 functions)
from .company import *  # noqa: F401, F403

# 5. fixed_income  (81 unique + 3 duplicates overridden later by market)
from .fixed_income import *  # noqa: F401, F403

# 6. market  (9 functions — canonical for yield curve functions)
from .market import *  # noqa: F401, F403

# 7. instruments  (14 functions)
from .instruments import *  # noqa: F401, F403

# 8. jobs  (16 functions — canonical for sync run functions)
from .jobs import *  # noqa: F401, F403

# 9. user  (5 functions — canonical for get_symbol_activity)
from .user import *  # noqa: F401, F403

# 10. etf  (7 functions)
from .etf import *  # noqa: F401, F403

# 11. treasury  (10 functions)
from .treasury import *  # noqa: F401, F403

# 12. yield_curves  (10 functions)
from .yield_curves import *  # noqa: F401, F403

# 13. fx_extended  (8 functions)
from .fx_extended import *  # noqa: F401, F403

# 14. dividends  (5 functions)
from .dividends import *  # noqa: F401, F403

# 15. ipo  (6 functions)
from .ipo import *  # noqa: F401, F403

# 16. vps  (4 functions)
from .vps import *  # noqa: F401, F403

# 17. website_scan  (4 functions)
from .website_scan import *  # noqa: F401, F403

# 18. financials  (7 functions)
from .financials import *  # noqa: F401, F403

# 19. tick  (8 functions)
from .tick import *  # noqa: F401, F403
