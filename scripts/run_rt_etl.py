"""RT ETL: polluje feed GTFS-RT i ładuje opóźnienia.

Użycie:
    python scripts/run_rt_etl.py           # ciągła pętla
    python scripts/run_rt_etl.py --once    # jeden cykl (do testów)
"""

import sys

from gtfs_olap.rt import run_loop

if __name__ == "__main__":
    once = "--once" in sys.argv
    run_loop(once=once)