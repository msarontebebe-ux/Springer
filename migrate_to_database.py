"""
Backfill existing silver JSON files into the Neon PostgreSQL database.

Reads every *_cleaned.json from data/silver/, reconstructs Publication objects,
and reuses the same _write_silver() / _refresh_gold() functions that the live
scraper uses — so the DB ends up in exactly the same state as a fresh scrape.

Safe to re-run: DOI-based deduplication skips already-inserted records.
"""

import json
from pathlib import Path
from datetime import datetime

from models_db import ScrapedPublication as Publication, ScrapedAuthor as Author

SILVER_DIR = Path("data/silver")


def load_publications_from_json(filepath: Path) -> list[Publication]:
    """Reconstruct Publication objects from a silver layer JSON file."""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    publications = []
    for rec in data.get("publications", []):
        authors = [
            Author(name=a.get("name", "").strip())
            for a in rec.get("authors", [])
            if a.get("name", "").strip()
        ]
        pub = Publication(
            title=rec.get("title", "Unknown Title"),
            authors=authors,
            year=rec.get("year"),
            venue=rec.get("venue") or rec.get("series"),
            series=rec.get("series"),
            volume=rec.get("volume"),
            pages=rec.get("pages"),
            doi=rec.get("doi"),
            ee=rec.get("ee") or rec.get("url"),
            isbn=rec.get("isbn"),
            publisher=rec.get("publisher", "Springer"),
            event_date_start=rec.get("event_date_start"),
            event_date_end=rec.get("event_date_end"),
            event_year=rec.get("event_year"),
            event_month=rec.get("event_month"),
            event_date_confidence=rec.get("event_date_confidence"),
        )
        publications.append(pub)

    return publications


def main():
    from main_crossref import _write_silver, _refresh_gold

    print("=" * 60)
    print("Silver JSON -> Neon Database Migration")
    print("=" * 60)

    files = sorted(SILVER_DIR.glob("*_cleaned.json"))
    if not files:
        print(f"No files found in {SILVER_DIR}. Run the scraper first.")
        return

    total_inserted = 0
    migrated_series = []

    for filepath in files:
        series_abbr = filepath.stem.replace("_cleaned", "")
        print(f"\n[{series_abbr}] Loading {filepath.name}...")

        publications = load_publications_from_json(filepath)
        if not publications:
            print(f"  No publications found — skipping")
            continue

        print(f"  {len(publications)} publications loaded from JSON")

        # Reuse the exact same Silver write logic as the live scraper
        series_name = publications[0].series or series_abbr
        inserted = _write_silver(publications, series_name)
        total_inserted += inserted
        migrated_series.append(series_name)

    print(f"\n{'='*60}")
    print(f"Migration complete: {total_inserted} new records inserted into Neon")
    print(f"{'='*60}")

    # Recompute Gold layer + quality metrics for all series
    print("\nRecomputing gold layer + quality metrics...")
    from gold_layer import refresh_gold
    refresh_gold()
    print("Gold layer ready.")


if __name__ == "__main__":
    main()
