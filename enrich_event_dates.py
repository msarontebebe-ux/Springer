"""
Enrich existing DB records with event dates extracted from CrossRef subtitles.

The original scrape omitted the subtitle field, which is where Springer stores
the full conference string (e.g. "October 21-22, 2024, Proceedings").
This script re-queries CrossRef by ISSN (bulk, paginated) fetching only
DOI + subtitle + event, then updates records that are missing event_date_start.
"""

import json
import time
import requests
from pathlib import Path
from sqlalchemy import text
from database_config import DatabaseConfig
from event_date_extractor import ConferenceDateExtractor

CROSSREF_URL = "https://api.crossref.org/works"
DELAY = 0.5  # seconds between pages (polite pool)
ROWS = 100


def fetch_subtitles_for_issn(session_http, issn: str, series_name: str) -> dict:
    """
    Fetch DOI -> subtitle mapping from CrossRef for a given ISSN.
    Returns {doi: subtitle_string} for records that have a subtitle.
    """
    cursor = "*"
    doi_to_subtitle = {}
    page = 0

    while True:
        params = {
            "filter": f"issn:{issn},type:book",
            "rows": ROWS,
            "cursor": cursor,
            "select": "DOI,subtitle,event",
        }
        try:
            r = session_http.get(CROSSREF_URL, params=params, timeout=30)
            r.raise_for_status()
            message = r.json().get("message", {})
            items = message.get("items", [])
            next_cursor = message.get("next-cursor")

            for item in items:
                doi = item.get("DOI")
                subtitle_list = item.get("subtitle", [])
                subtitle = subtitle_list[0] if subtitle_list else None
                if doi and subtitle:
                    doi_to_subtitle[doi] = subtitle

            page += 1
            fetched = page * ROWS
            print(f"    page {page}: {fetched} records scanned, "
                  f"{len(doi_to_subtitle)} with subtitle so far...", end="\r")

            if not items or not next_cursor:
                break
            cursor = next_cursor
            time.sleep(DELAY)

        except Exception as exc:
            print(f"\n    Warning: API error on page {page}: {exc}")
            break

    print()  # newline after progress
    return doi_to_subtitle


def update_event_dates(db_engine, doi_to_subtitle: dict) -> int:
    """
    For each DOI with a subtitle, extract dates and update the DB if missing.
    Returns count of rows updated.
    """
    updated = 0
    with db_engine.connect() as conn:
        for doi, subtitle in doi_to_subtitle.items():
            info = ConferenceDateExtractor.extract_from_title(subtitle)
            if not info or info.get("confidence") == "low":
                continue
            if not info.get("event_date_start") and not info.get("event_year"):
                continue

            result = conn.execute(text("""
                UPDATE publications
                SET event_date_start      = :start,
                    event_date_end        = :end,
                    event_year            = COALESCE(event_year, :year),
                    event_month           = COALESCE(event_month, :month),
                    event_date_confidence = :conf
                WHERE doi = :doi
                  AND event_date_start IS NULL
            """), {
                "doi": doi,
                "start": info.get("event_date_start"),
                "end": info.get("event_date_end"),
                "year": info.get("event_year"),
                "month": info.get("event_month"),
                "conf": info.get("confidence"),
            })
            updated += result.rowcount

        conn.commit()
    return updated


def main():
    config = json.load(open("config.json", encoding="utf-8"))

    session_http = requests.Session()
    email = config.get("crossref_api", {}).get("email", "research@example.com")
    session_http.headers["User-Agent"] = f"SpringerEnricher/1.0 (mailto:{email})"

    db_engine = DatabaseConfig.create_engine_from_env()

    print("=" * 60)
    print("Event Date Enrichment from CrossRef Subtitles")
    print("=" * 60)

    total_updated = 0

    for series in config.get("springer_series", []):
        name = series["name"]
        abbr = series["abbreviation"]
        issn = series.get("issn")

        if not series.get("scrape_via_issn", True):
            parent = series.get("issn_shared_with", "parent series")
            print(f"\n[{abbr}] Skipped — ISSN shared with {parent}, already enriched in that pass.")
            continue

        if not issn:
            print(f"\n[{abbr}] No ISSN configured — skipping")
            continue

        print(f"\n[{abbr}] Fetching subtitles from CrossRef (ISSN {issn})...")
        doi_to_subtitle = fetch_subtitles_for_issn(session_http, issn, name)
        print(f"  {len(doi_to_subtitle)} records have subtitle")

        if not doi_to_subtitle:
            continue

        updated = update_event_dates(db_engine, doi_to_subtitle)
        total_updated += updated
        print(f"  {updated} DB records updated with event dates")

    print(f"\n{'='*60}")
    print(f"Enrichment complete: {total_updated} records updated")
    print(f"{'='*60}")

    # Recompute gold quality metrics
    print("\nRecomputing quality metrics...")
    from gold_layer import refresh_gold
    refresh_gold()
    print("Done.")


if __name__ == "__main__":
    main()
