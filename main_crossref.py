"""Main script for scraping Springer publications using CrossRef API."""

import json
import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
from crossref_scraper import CrossRefScraper
from storage import DataLakeStorageHandler

def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)

# Load .env before anything else so DATABASE_URL is visible to _db_configured()
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def load_config(config_path: str = "config.json") -> dict:
    """Load configuration from JSON file."""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _db_configured() -> bool:
    """Return True when cloud DB credentials are present."""
    return bool(
        os.getenv('AWS_RDS_CONNECTION_STRING')
        or os.getenv('AZURE_SQL_CONNECTION_STRING')
        or os.getenv('DATABASE_URL')
        or os.getenv('DB_TYPE', '').lower() == 'postgresql'
    )


def _write_bronze(raw_data: list, series_abbr: str, series_name: str):
    """Store raw API response in bronze.raw_responses."""
    if not raw_data:
        return
    try:
        from database_config import DatabaseConfig
        from models_db import BronzeRawResponse
        session = DatabaseConfig.get_session()
        # Store only a sample (first 10 records) — full raw data lives in data/bronze/ files.
        # Storing the entire batch in one DB cell hits Neon's 64 MB response limit.
        session.add(BronzeRawResponse(
            series_abbr=series_abbr,
            series_name=series_name,
            fetched_at=_utcnow(),
            record_count=len(raw_data),
            raw_data=raw_data[:10],
        ))
        session.commit()
        session.close()
        print(f"   Bronze: {len(raw_data)} raw records saved to Neon")
    except Exception as exc:
        print(f"   Bronze write failed: {exc}")


def _write_silver(publications, series_name: str) -> int:
    """
    Upsert cleaned publications into silver tables in batches of 100.
    Each batch is its own committed transaction to avoid Neon serverless timeouts.
    Author cache stores IDs (not ORM objects) to avoid detached-instance errors
    across sessions.
    Returns total count inserted.
    """
    from database_config import DatabaseConfig
    from models_db import Publication, Author
    from sqlalchemy import text as sa_text

    BATCH_SIZE = 100
    pub_dicts = [p.to_dict() for p in publications]
    total_inserted = 0
    total_skipped = 0

    # Pre-load existing DOIs and author name→id into plain Python dicts/sets.
    # Plain Python types are safe to use across sessions — no detached ORM objects.
    session = DatabaseConfig.get_session()
    try:
        existing_dois = {
            row[0] for row in session.execute(sa_text('SELECT doi FROM publications'))
        }
        # Cache: author name → integer ID (not ORM object)
        author_name_to_id = {
            row[0]: row[1]
            for row in session.execute(sa_text('SELECT name, id FROM authors'))
        }
    finally:
        session.close()

    # Process in committed batches of BATCH_SIZE.
    # No session.flush() inside the loop — SQLAlchemy assigns IDs at commit time.
    # Authors created in this batch are tracked by name until the batch commits,
    # then their real IDs are written into author_name_to_id for future batches.
    for batch_start in range(0, len(pub_dicts), BATCH_SIZE):
        batch = pub_dicts[batch_start: batch_start + BATCH_SIZE]
        session = DatabaseConfig.get_session()
        batch_inserted = 0
        # name → Author object for authors created within this batch (no ID yet)
        new_authors_this_batch: dict = {}
        try:
            for pub_data in batch:
                doi = pub_data.get('doi')
                if not doi or doi in existing_dois:
                    total_skipped += 1
                    continue

                pub = Publication(
                    doi=doi,
                    title=pub_data.get('title', ''),
                    year=pub_data.get('year'),
                    series=pub_data.get('series'),
                    volume=pub_data.get('volume'),
                    pages=pub_data.get('pages'),
                    isbn=pub_data.get('isbn'),
                    publisher=pub_data.get('publisher', 'Springer'),
                    url=pub_data.get('ee') or pub_data.get('url'),
                    event_date_start=pub_data.get('event_date_start'),
                    event_date_end=pub_data.get('event_date_end'),
                    event_year=pub_data.get('event_year'),
                    event_month=pub_data.get('event_month'),
                    event_date_confidence=pub_data.get('event_date_confidence'),
                    fetched_at=(
                        datetime.fromisoformat(pub_data['fetched_at'])
                        if pub_data.get('fetched_at') else _utcnow()
                    ),
                )
                session.add(pub)

                # Deduplicate authors per publication by name to avoid
                # duplicate (publication_id, author_id) constraint violations
                seen_author_names: set = set()

                for author_data in pub_data.get('authors', []):
                    name = (
                        author_data.get('name', '')
                        if isinstance(author_data, dict) else str(author_data)
                    ).strip()
                    if not name or name in seen_author_names:
                        continue
                    seen_author_names.add(name)

                    if name in author_name_to_id:
                        # Already in DB — load by ID into this session
                        author = session.get(Author, author_name_to_id[name])
                    elif name in new_authors_this_batch:
                        # Created earlier in this same batch — reuse the object
                        author = new_authors_this_batch[name]
                    else:
                        # Brand new author — create and track for this batch
                        author = Author(name=name)
                        session.add(author)
                        new_authors_this_batch[name] = author

                    pub.authors.append(author)

                existing_dois.add(doi)
                batch_inserted += 1

            # Single commit per batch — SQLAlchemy assigns all IDs here
            session.commit()
            total_inserted += batch_inserted

            # Now IDs are assigned — persist new authors into the cross-batch cache
            for name, author in new_authors_this_batch.items():
                author_name_to_id[name] = author.id
            total_skipped += (len(batch) - batch_inserted)

        except Exception as exc:
            session.rollback()
            print(f"   Silver: batch {batch_start}-{batch_start+BATCH_SIZE} failed: {exc}")
        finally:
            session.close()

        # Progress for large series
        done = batch_start + len(batch)
        if len(pub_dicts) > BATCH_SIZE:
            print(f"   Silver: {done}/{len(pub_dicts)} processed, {total_inserted} inserted...", end='\r')

    if len(pub_dicts) > BATCH_SIZE:
        print()  # newline after progress line
    print(f"   Silver: inserted={total_inserted}  skipped={total_skipped}")
    return total_inserted


def _refresh_gold(series_name: str):
    """Recompute gold aggregations for this series."""
    try:
        from gold_layer import refresh_gold
        refresh_gold(series_filter=series_name)
    except Exception as exc:
        print(f"   Gold compute failed: {exc}")


def _log_pipeline_run(series: str, records_fetched: int, records_new: int,
                      status: str, error: str = None,
                      started_at: datetime = None):
    """Write a row to pipeline_runs for observability."""
    try:
        from database_config import DatabaseConfig
        from models_db import PipelineRun
        session = DatabaseConfig.get_session()
        session.add(PipelineRun(
            run_type='incremental',
            series=series,
            records_fetched=records_fetched,
            records_new=records_new,
            status=status,
            error_message=error,
            started_at=started_at or _utcnow(),
            completed_at=_utcnow(),
        ))
        session.commit()
        session.close()
    except Exception as exc:
        print(f"   Pipeline log failed: {exc}")


def scrape_single_series(series_config: dict, scraper: CrossRefScraper,
                        storage: DataLakeStorageHandler, year_start=None, year_end=None,
                        max_results=50000):
    """
    Scrape a single Springer series using CrossRef.
    
    Args:
        series_config: Series configuration dictionary
        scraper: CrossRefScraper instance
        storage: DataLakeStorageHandler instance
        year_start: Optional start year filter
        year_end: Optional end year filter
        max_results: Maximum results to fetch
    """
    name = series_config['name']
    abbr = series_config['abbreviation']
    issn = series_config.get('issn')

    print(f"\n{'='*80}")
    print(f"Scraping: {name} ({abbr})")
    print(f"{'='*80}")

    # Skip series whose ISSN is shared with a parent series — querying it would
    # return duplicate records already captured by the parent scrape, and
    # CrossRef provides no field to distinguish them (confirmed: container-title,
    # ISSN, and short-container-title are identical for LNCS/LNAI/LNBI).
    if not series_config.get('scrape_via_issn', True) and not issn:
        parent = series_config.get('issn_shared_with', 'parent series')
        print(f"  Skipped: ISSN shared with {parent} — records captured in that scrape.")
        print(f"  Note: {series_config.get('note', '')}")
        return []

    publications = []
    raw_data = []

    # Try ISSN-based search first (more reliable)
    if issn:
        print(f"Using ISSN: {issn}")
        publications, raw_data = scraper.search_by_issn(issn, year_start, year_end, max_results)
        
        # Also try electronic ISSN if available
        if not publications and 'issn_electronic' in series_config:
            e_issn = series_config['issn_electronic']
            print(f"Trying electronic ISSN: {e_issn}")
            publications, raw_data = scraper.search_by_issn(e_issn, year_start, year_end, max_results)
    
    # Fallback to title-based search
    if not publications:
        print(f"Falling back to series title search")
        # Title search doesn't return raw data yet (can be added if needed)
        publications = scraper.search_by_series_title(
            name, "Springer", year_start, year_end, max_results
        )
        raw_data = []  # No raw data from title search
    
    if publications:
        # Set series name for all publications
        for pub in publications:
            pub.series = name

        started = _utcnow()
        records_new = 0
        status = 'success'
        error = None

        if _db_configured():
            # --- Cloud pipeline: Bronze → Silver → Gold → Log ---
            print(f"\n  Writing to Neon (3 layers):")
            _write_bronze(raw_data, abbr, name)
            records_new = _write_silver(publications, name)
            _refresh_gold(name)
            _log_pipeline_run(name, len(publications), records_new, status, started_at=started)
            print(f"Done: {abbr}: {len(publications)} fetched, {records_new} new in Neon")
        else:
            # --- Local fallback: write files only ---
            print(f"\n  No DB configured — saving to local files only")
            storage.save_by_series(publications, raw_data, abbr)
            print(f"Done: {abbr}: {len(publications)} fetched, saved to local files")
    else:
        print(f"  No publications found for {abbr}")
    
    return publications


def scrape_all_series(config: dict, year_start=None, year_end=None, max_results=50000):
    """
    Scrape all configured Springer series.
    
    Args:
        config: Configuration dictionary
        year_start: Optional start year filter
        year_end: Optional end year filter
        max_results: Maximum results per series
    """
    crossref_config = config['crossref_api']
    output_config = config['output']
    
    scraper = CrossRefScraper(
        email=crossref_config['email'],
        delay=crossref_config['delay']
    )
    storage = DataLakeStorageHandler(base_dir=output_config['directory'])
    
    all_publications = []
    series_summary = []
    
    for series_config in config['springer_series']:
        publications = scrape_single_series(
            series_config, scraper, storage, year_start, year_end, max_results
        )
        all_publications.extend(publications)
        series_summary.append({
            'series': series_config['abbreviation'],
            'name': series_config['name'],
            'count': len(publications)
        })
    
    # Save combined data to silver layer
    if all_publications:
        storage.save_silver_json(all_publications, "all_springer_publications_crossref")
        storage.save_silver_csv(all_publications, "all_springer_publications_crossref")
    
    # Print summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    for item in series_summary:
        print(f"  {item['series']:10} - {item['name']:55} : {item['count']:5} publications")
    print(f"\n  TOTAL: {len(all_publications)} publications")
    print(f"{'='*80}\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Scrape Springer publication metadata from CrossRef API'
    )
    parser.add_argument(
        '--series',
        help='Scrape specific series by abbreviation (e.g., LNCS, CCIS)',
        type=str
    )
    parser.add_argument(
        '--year-start',
        help='Start year for filtering (inclusive)',
        type=int
    )
    parser.add_argument(
        '--year-end',
        help='End year for filtering (inclusive)',
        type=int
    )
    parser.add_argument(
        '--max-results',
        help='Maximum results per series (default: 50000)',
        type=int,
        default=50000
    )
    parser.add_argument(
        '--config',
        help='Path to configuration file (default: config.json)',
        type=str,
        default='config.json'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    crossref_config = config['crossref_api']
    output_config = config['output']
    
    scraper = CrossRefScraper(
        email=crossref_config['email'],
        delay=crossref_config['delay']
    )
    storage = DataLakeStorageHandler(base_dir=output_config['directory'])
    
    if args.series:
        # Scrape specific series
        series_config = next(
            (s for s in config['springer_series'] 
             if s['abbreviation'].upper() == args.series.upper()),
            None
        )
        
        if not series_config:
            print(f"Error: Series '{args.series}' not found in configuration")
            print(f"Available series: {', '.join([s['abbreviation'] for s in config['springer_series']])}")
            return
        
        scrape_single_series(
            series_config, scraper, storage, 
            args.year_start, args.year_end, args.max_results
        )
    else:
        # Scrape all series
        scrape_all_series(config, args.year_start, args.year_end, args.max_results)


if __name__ == '__main__':
    main()
