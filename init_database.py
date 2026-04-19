"""
Initialize the cloud database.
Creates bronze / public (silver) / gold schemas and all tables,
then seeds series metadata from config.json.

Run once after setting .env credentials, or any time you add new tables.
Safe to re-run — uses CREATE IF NOT EXISTS semantics.
"""

import json
from pathlib import Path
from sqlalchemy import text
from database_config import DatabaseConfig
from models_db import Base, Series


def init_database():
    print("=" * 60)
    print("Database Initialization")
    print("=" * 60)

    # 1. Connect
    print("\n1. Connecting to database...")
    engine = DatabaseConfig.create_engine_from_env()
    print(f"   {engine.url}")

    # 2. Ensure schemas exist (DDL events on Base handle this, but be explicit)
    print("\n2. Creating schemas: bronze, gold...")
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
        conn.commit()
    print("   Schemas ready")

    # 3. Create all tables
    print("\n3. Creating tables...")
    Base.metadata.create_all(engine)

    tables = {
        'bronze': ['raw_responses'],
        'public (silver)': ['publications', 'authors', 'publication_authors',
                            'series', 'pipeline_runs', 'quality_metrics'],
        'gold': ['series_stats', 'yearly_trends', 'top_authors'],
    }
    for schema, tbl_list in tables.items():
        print(f"   {schema}: {', '.join(tbl_list)}")

    # 4. Seed series from config.json
    print("\n4. Seeding series metadata...")
    config_path = Path("config.json")
    if config_path.exists():
        with open(config_path) as f:
            config = json.load(f)

        session = DatabaseConfig.get_session()
        for s in config.get('springer_series', []):
            session.merge(Series(
                abbreviation=s['abbreviation'],
                full_name=s['name'],
                issn=s.get('issn'),
                issn_electronic=s.get('issn_electronic'),
                springer_series_id=s.get('springer_series_id'),
                springer_url=s.get('springer_url'),
            ))
            print(f"   {s['abbreviation']:12} {s['name']}")
        session.commit()
        session.close()
    else:
        print("   config.json not found — skipping")

    print("\n" + "=" * 60)
    print("Done! All schemas and tables are ready.")
    print("Next: python migrate_to_database.py   (if you have existing local data)")
    print("      python main_crossref.py --series LNCS --year-start 2024")
    print("=" * 60)


if __name__ == '__main__':
    init_database()
