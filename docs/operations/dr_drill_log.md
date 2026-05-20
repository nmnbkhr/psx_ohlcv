# DR Drill Log

Quarterly disaster-recovery drill results.

The drill exercises `scripts/sqlite_page_recover.py` against a
corrupted copy of the live `psx.sqlite` to keep the recovery path
sharp. **The live DB is never touched** — every drill workspace is
under `/mnt/e/psxdata/dr_drill_<date>/` and `dr_drill.sh`'s safety
guard refuses any path under `~/psxdata_rescue/`.

See [`docs/runbooks/db_corruption.md`](../runbooks/db_corruption.md)
for the full corruption-recovery runbook.

## Schedule

| Quarter | Target month | Status |
|---------|--------------|--------|
| Q2 2026 | May 2026     | ✅ done — see entry below |
| Q3 2026 | Aug 2026     | pending |
| Q4 2026 | Nov 2026     | pending |
| Q1 2027 | Feb 2027     | pending |

Pin a reminder on the calendar at the start of each quarter; the drill
itself takes ~30 min (cheap-path: prepare + corrupt + scoped recover +
verify).

## Drill command

```bash
DATE=$(date +%Y%m%d)
~/projects/pakfindata/scripts/dr_drill.sh prepare $DATE
~/projects/pakfindata/scripts/dr_drill.sh corrupt $DATE
~/projects/pakfindata/scripts/dr_drill.sh recover $DATE \
    --tables eod_ohlcv,psx_indices,kibor_daily,sovereign_curve
~/projects/pakfindata/scripts/dr_drill.sh verify  $DATE

cat /mnt/e/psxdata/dr_drill_$DATE/verification_report.txt
```

After the drill, append a new entry below and commit.

---

## 2026-05-20 — initial drill (Q2 2026)

| Metric | Value |
|---|---|
| Live DB size | 14.21 GB (3,469,132 pages × 4 KB) |
| Workspace | `/mnt/e/psxdata/dr_drill_20260520/` (NTFS-3g via FUSE) |
| Prepare time | 2m 55s (sqlite3 `.backup` API) |
| Corrupt time | 6m 5s (cp + 16-byte header overwrite via dd) |
| Recover wall time | 1h 9m 9s (4,149s) — SIGTERMed at 18.7% of pages |
| Pages scanned | 652,788 / 3,469,132 (18.7%) |
| Scan rate | 185–294 pg/s (decreasing under FUSE cache pressure) |
| Projected full-scan time | ~5 hours (NTFS-3g, this laptop) |
| Recovered rows | 17,545,003 of 59,619,593 (29.4% overall) |
| Recovered DB size | 2.6 GB |
| Verify time | ~10 min (155 tables × 2 COUNT queries) |

### High-value tables — all 98-100% recovered from partial scan

| Table | Rows orig | Rows recov | % |
|---|---:|---:|---:|
| `eod_ohlcv` | 615,392 | 606,704 | **98.6%** |
| `tick_data` | 10,048,488 | 10,048,488 | **100%** |
| `pib_auctions` | 241,988 | 241,988 | **100%** |
| `fi_quotes` | 132,288 | 132,288 | **100%** |
| `sovereign_curve` | 41,747 | 41,734 | **100.0%** |
| `kibor_daily` | 48,742 | 48,766 | **100%** |
| `corporate_announcements` | 10,489 | 10,489 | **100%** |
| `sbp_benchmark_snapshot` | 99,309 | 99,291 | **100%** |
| `fund_performance` | 499,873 | 498,134 | **99.7%** |
| `futures_eod` | 370,839 | 360,213 | **97.1%** |

### Late-page tables — would recover in remaining 81% of scan

| Table | Rows orig | Rows recov |
|---|---:|---:|
| `intraday_bars` | 11,446,574 | 62 |
| `eod_*_summary` family | 469,589 (combined) | 0 |
| `intraday_*_summary` family | 220,504 (combined) | 0 |
| `sbp_fx_{daily,monthly}_avg` | 91,227 (combined) | 0 |
| `regular_market_snapshots` | 3,158 | 0 |
| `sectors` | 74 | 0 |

These are derived / late-arriving tables. After a real recovery, run
`daily_sync.sh` to repopulate them from sources (the master rates and
EOD data are intact in the partial recovery).

### Findings from this drill

1. **Recovery tool works correctly** on the May-9 failure mode
   (16-byte header destroyed, file unrecognizable to sqlite3).
2. **NTFS-3g via FUSE is the bottleneck**, not the recovery tool.
   Same recovery on an ext4 mount would be ~10× faster, but the
   live disk (NVMe at `/home/smnb/psxdata_rescue/`) doesn't have
   42 GB free for a parallel workspace.
3. **`--tables` flag works for emergency targeted recovery.** A real
   emergency should run a 20-30-min targeted recovery for high-value
   tables first, write a usable DB, then resume the full scan overnight.
4. **`PRAGMA integrity_check` is unusable on 14GB NTFS** (>30 min);
   `PRAGMA quick_check` and the recovery tool's own report are the
   right verification path.
5. **`SELECT 1` is not a corruption detector** (constant expression);
   use `SELECT name FROM sqlite_master LIMIT 1`.
6. **`sqlite_page_recover.py` handles SIGTERM gracefully** — finishes
   the current page, writes checkpoint, exits clean. Re-run resumes
   from `last_page_scanned`.
7. **Bash dispatch bug found and fixed** during the drill — earlier
   `dr_drill.sh` versions dropped pass-through args (`--dry-run`,
   `--tables`). Fixed in commit 0a3bab2.
8. **The backup cron silently misses days** when the laptop is asleep
   at 02:00 PKT. Yesterday's backup is not guaranteed to exist; check
   `~/.cron_backup.log` before relying on a timestamp.

### Action items from this drill

- ☐ Phase 1: investigate running the backup via `anacron` (catch-up
  semantics) or as the first step of `daily_sync.sh` (which runs at
  03:45 PKT and usually sees the laptop awake).
- ☐ Phase 1: add a `--tables-from-priority-list` flag to
  `sqlite_page_recover.py` so emergency recoveries don't require
  remembering the canonical list.
- ☐ Future: explore moving DB onto NVMe with explicit free-space
  monitoring so the workspace can also live there during emergencies.
