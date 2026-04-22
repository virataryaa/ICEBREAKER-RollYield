"""
Roll Yield Ingest — ICEBREAKER
Fetches Settle prices for 11 commodities via ICE Connect (icepython).
Output: ../Database/roll_yield_data.parquet
Schema: Date, Commodity, Spot, OneYr, Roll_Yield_1yr, c1-c8
"""
import datetime
import logging
import pandas as pd
import icepython as ice
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

OUT        = Path(__file__).parent.parent / "Database" / "roll_yield_data.parquet"
TODAY      = datetime.date.today().isoformat()
FULL_START = "2016-01-01"

# (key, name, spot_sym, yr1_sym, curve c1→c8)
COMMODITIES = [
    ("KC",  "Arabica",      "%KC 2!",      "%KC 7!",      [f"%KC {i}!"      for i in range(1, 9)]),
    ("RC",  "Robusta",      "%RC 2!-ICE",  "%RC 8!-ICE",  [f"%RC {i}!-ICE"  for i in range(1, 9)]),
    ("CC",  "NYC Cocoa",    "%CC 2!",      "%CC 7!",      [f"%CC {i}!"      for i in range(1, 9)]),
    ("LCC", "LDN Cocoa",    "%C 2!-ICE",   "%C 7!-ICE",   [f"%C {i}!-ICE"   for i in range(1, 9)]),
    ("SB",  "Sugar",        "%SB 1!",      "%SB 5!",      [f"%SB {i}!"      for i in range(1, 9)]),
    ("CT",  "Cotton",       "%CT 2!",      "%CT 7!",      [f"%CT {i}!"      for i in range(1, 9)]),
    ("W",   "White Sugar",  "%W 1!-ICE",   "%W 6!-ICE",   [f"%W {i}!-ICE"   for i in range(1, 9)]),
    ("ZC",  "Corn",         "%ZC 1!",      "%ZC 6!",      [f"%ZC {i}!"      for i in range(1, 9)]),
    ("ZW",  "Wheat",        "%ZW 1!",      "%ZW 6!",      [f"%ZW {i}!"      for i in range(1, 9)]),
    ("KE",  "KC Wheat",     "%KE 1!",      "%KE 6!",      [f"%KE {i}!"      for i in range(1, 9)]),
    ("JO",  "OJ",           "%JO 2!",      "%JO 7!",      [f"%JO {i}!"      for i in range(1, 9)]),
]


def _start() -> tuple[str, bool]:
    if OUT.exists():
        last = pd.to_datetime(pd.read_parquet(OUT, columns=["Date"])["Date"]).max().date()
        start = (last - datetime.timedelta(days=10)).isoformat()
        log.info(f"Incremental: from {start} (last={last})")
        return start, True
    log.info(f"Full history from {FULL_START}")
    return FULL_START, False


def _fetch(sym: str, start: str) -> pd.Series:
    try:
        raw = ice.get_timeseries(sym, ["Settle"], granularity="D", start_date=start, end_date=TODAY)
        if not raw or len(raw) < 2:
            return pd.Series(dtype=float, name=sym)
        header, rows = raw[0], raw[1:]
        df = pd.DataFrame(rows, columns=header)
        df["Time"] = pd.to_datetime(df["Time"])
        df = df.set_index("Time")
        return df[header[1]].rename(sym)
    except Exception as e:
        log.warning(f"  {sym}: {e}")
        return pd.Series(dtype=float, name=sym)


def build(start: str) -> pd.DataFrame:
    rows = []
    for comm, name, spot_sym, yr1_sym, curve in COMMODITIES:
        log.info(f"{name} ({comm})")
        data = {sym: _fetch(sym, start) for sym in curve}
        df = pd.DataFrame(data).dropna(subset=[spot_sym, yr1_sym])
        if df.empty:
            log.warning(f"  {comm}: no data — skipping")
            continue
        df.index.name = "Date"
        df = df.reset_index()
        df["Commodity"]      = comm
        df["Spot"]           = df[spot_sym]
        df["OneYr"]          = df[yr1_sym]
        df["Roll_Yield_1yr"] = df[spot_sym] / df[yr1_sym] - 1
        df = df.rename(columns={sym: f"c{i+1}" for i, sym in enumerate(curve)})
        keep = ["Date", "Commodity", "Spot", "OneYr", "Roll_Yield_1yr"] + [f"c{i}" for i in range(1, 9)]
        rows.append(df[[c for c in keep if c in df.columns]])
        log.info(f"  {len(df)} rows | roll = {df['Roll_Yield_1yr'].iloc[-1]:.2%}")
    return pd.concat(rows, ignore_index=True)


def save(new: pd.DataFrame, incremental: bool):
    if incremental and OUT.exists():
        old = pd.read_parquet(OUT)
        old["Date"] = pd.to_datetime(old["Date"])
        old = old[old["Date"] < new["Date"].min()]
        df = pd.concat([old, new], ignore_index=True).sort_values(["Commodity", "Date"])
        log.info(f"Appended {len(new)} rows to {len(old)} existing")
    else:
        df = new
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT, index=False)
    log.info(f"Saved {len(df):,} rows → {OUT}")
    log.info(f"Range: {df['Date'].min().date()} → {df['Date'].max().date()}")


if __name__ == "__main__":
    log.info("=" * 50 + f"\nRoll Yield Ingest | {TODAY}\n" + "=" * 50)
    start, incremental = _start()
    new = build(start)
    save(new, incremental)
    log.info("=" * 50)
