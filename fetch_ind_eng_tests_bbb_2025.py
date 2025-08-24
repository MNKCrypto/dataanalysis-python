# fetch_ind_eng_tests_bbb_2025.py
# Download ball-by-ball for India vs England Tests (2025) from Cricsheet JSON and save as a CSV.

import io
import os
import re
import zipfile
import json
from datetime import date, datetime
import requests
import pandas as pd
from tqdm import tqdm

CRICSHEET_TESTS_ZIP = "https://cricsheet.org/downloads/tests_json.zip"  # public dataset
OUT_CSV = "ind_eng_2025_tests_balls.csv"

# Date window for the 2025 England vs India Test series
START = date(2025, 6, 1)
END   = date(2025, 8, 31)

def within_series_window(info):
    """Cricsheet 'info.dates' is a list of ISO dates (strings)."""
    ds = info.get("dates") or []
    for d in ds:
        try:
            dt = datetime.fromisoformat(str(d)).date()
            if START <= dt <= END:
                return True
        except Exception:
            continue
    return False

def is_ind_eng_test(info):
    if (info.get("match_type") or info.get("match_type_number")) and (info.get("match_type") or "").lower() != "test":
        return False
    teams = set((info.get("teams") or []))
    return {"India", "England"}.issubset(teams)

def stream_zip_bytes(url):
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    return io.BytesIO(r.content)

def flatten_match(json_obj):
    """
    Cricsheet JSON (new format):
      {
        "meta": {...},
        "info": {...},
        "innings": [
          {"team": "England", "overs": [
               {"over": 0, "deliveries":[
                   {"ball": 0.1, "batter": "...", "bowler": "...", "non_striker": "...",
                    "runs": {"batter":0,"extras":0,"total":0}, "extras":{"legbyes":1}, "wickets":[...]},
                   ...
               ]},
               ...
          ]},
          ...
        ]
      }
    """
    info = json_obj.get("info", {})
    match_id = info.get("match_id") or info.get("event", {}).get("match_number")  # match_id is present in new JSON
    venue = info.get("venue")
    city = info.get("city")
    teams = info.get("teams")
    dates = info.get("dates")
    season = info.get("season")
    balls = []

    for inn_no, inn in enumerate(json_obj.get("innings", []), start=1):
        team = inn.get("team")
        for over_block in inn.get("overs", []):
            over_no = over_block.get("over")
            for d in over_block.get("deliveries", []):
                runs = d.get("runs", {})
                wicket_list = d.get("wickets", []) or []
                extras = d.get("extras", {}) or {}
                balls.append({
                    "match_id": match_id,
                    "season": season,
                    "date_list": ",".join(map(str, dates or [])),
                    "venue": venue,
                    "city": city,
                    "teams": ",".join(teams or []),

                    "innings": inn_no,
                    "batting_team": team,
                    "over": over_no,
                    "ball_label": d.get("ball"),  # string like "12.3" or float; keep as-is
                    "batter": d.get("batter"),
                    "bowler": d.get("bowler"),
                    "non_striker": d.get("non_striker"),

                    "runs_batter": runs.get("batter", 0),
                    "runs_extras": runs.get("extras", 0),
                    "runs_total": runs.get("total", 0),

                    # extras breakdown (may be absent)
                    "extra_byes": extras.get("byes", 0),
                    "extra_legbyes": extras.get("legbyes", 0),
                    "extra_wides": extras.get("wides", 0),
                    "extra_noballs": extras.get("noballs", 0),
                    "extra_penalty": extras.get("penalty", 0),

                    # wicket info (flatten first if multiple)
                    "wicket_kind": wicket_list[0].get("kind") if wicket_list else None,
                    "wicket_player_out": wicket_list[0].get("player_out") if wicket_list else None,
                    "wicket_fielders": ",".join([w.get("fielders", [{}])[0].get("name","")
                                                 for w in wicket_list if w.get("fielders")]) if wicket_list else None,
                })
    return balls

def main():
    print("Downloading Cricsheet Tests JSON zip ...")
    buf = stream_zip_bytes(CRICSHEET_TESTS_ZIP)

    rows = []
    with zipfile.ZipFile(buf) as zf:
        # Iterate all JSON files, filter to Ind v Eng within window
        json_files = [n for n in zf.namelist() if n.endswith(".json")]
        for name in tqdm(json_files, desc="Scanning matches"):
            with zf.open(name) as f:
                try:
                    obj = json.load(f)
                except Exception:
                    continue
            info = obj.get("info", {})
            if not is_ind_eng_test(info):
                continue
            if not within_series_window(info):
                continue
            rows.extend(flatten_match(obj))

    if not rows:
        raise SystemExit("No India vs England Test matches found in the specified window. "
                         "Try widening START/END or verify series dates.")

    df = pd.DataFrame(rows)
    # Optional tidy-ups
    # Ensure numeric over/ball columns
    if "over" in df.columns:
        df["over"] = pd.to_numeric(df["over"], errors="coerce").astype("Int64")
    if "runs_total" in df.columns:
        df["runs_total"] = pd.to_numeric(df["runs_total"], errors="coerce").fillna(0).astype(int)

    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"Saved → {OUT_CSV} with {len(df):,} balls")

if __name__ == "__main__":
    main()
