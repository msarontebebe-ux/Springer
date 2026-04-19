"""
REST API for serving Springer publications from Neon PostgreSQL.
Falls back to local JSON files if DATABASE_URL is not configured.
"""

import json
import os
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

# Load .env first so DATABASE_URL is visible before anything else
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import text

app = Flask(__name__)
CORS(app)

DATA_DIR = Path("data")

# ---------------------------------------------------------------------------
# Series abbreviation → full name mapping (loaded from config.json)
# ---------------------------------------------------------------------------

def _load_series_map() -> Dict[str, str]:
    """Return {abbreviation: full_name} from config.json."""
    try:
        with open('config.json', encoding='utf-8') as f:
            cfg = json.load(f)
        return {s['abbreviation'].upper(): s['name'] for s in cfg.get('springer_series', [])}
    except Exception:
        return {}

SERIES_MAP = _load_series_map()   # e.g. {"LNCS": "Lecture Notes in Computer Science", ...}


def _resolve_series(abbr_or_name: str) -> str:
    """
    Accept either an abbreviation (LNCS) or a full name and always
    return the full name as stored in the DB.
    """
    if not abbr_or_name:
        return abbr_or_name
    upper = abbr_or_name.strip().upper()
    return SERIES_MAP.get(upper, abbr_or_name)


# ---------------------------------------------------------------------------
# Database availability check
# ---------------------------------------------------------------------------

def _db_available() -> bool:
    """True when a cloud DB connection string is present in the environment."""
    return bool(
        os.getenv('DATABASE_URL')
        or os.getenv('AWS_RDS_CONNECTION_STRING')
        or os.getenv('AZURE_SQL_CONNECTION_STRING')
        or os.getenv('DB_TYPE', '').lower() == 'postgresql'
    )


# ---------------------------------------------------------------------------
# Database-backed query helpers
# ---------------------------------------------------------------------------

def _get_session():
    from database_config import DatabaseConfig
    return DatabaseConfig.get_session()


def _search_db(series: str = None, year: int = None,
               query: str = None, limit: int = 100) -> List[Dict]:
    from models_db import Publication
    session = _get_session()
    try:
        q = session.query(Publication)
        if series:
            full_name = _resolve_series(series)
            q = q.filter(Publication.series.ilike(f'%{full_name}%'))
        if year:
            q = q.filter(Publication.year == year)
        if query:
            q = q.filter(Publication.title.ilike(f'%{query}%'))
        q = q.order_by(Publication.year.desc()).limit(limit)
        return [pub.to_dict() for pub in q.all()]
    finally:
        session.close()


def _series_stats_db() -> List[Dict]:
    """Return per-series counts from the gold layer."""
    session = _get_session()
    try:
        rows = session.execute(text("""
            SELECT series, total_publications, total_unique_authors,
                   year_min, year_max, avg_authors_per_paper
            FROM gold.series_stats
            ORDER BY total_publications DESC
        """)).fetchall()
        return [
            {
                'name':                 row[0],
                'abbreviation':         row[0],
                'total_publications':   row[1],
                'total_unique_authors': row[2],
                'year_min':             row[3],
                'year_max':             row[4],
                'avg_authors_per_paper': float(row[5]) if row[5] else 0,
            }
            for row in rows
        ]
    finally:
        session.close()


def _stats_db() -> Dict:
    session = _get_session()
    try:
        total        = session.execute(text('SELECT COUNT(*) FROM publications')).scalar()
        series_count = session.execute(text('SELECT COUNT(DISTINCT series) FROM publications')).scalar()
        year_min     = session.execute(text('SELECT MIN(year) FROM publications')).scalar()
        year_max     = session.execute(text('SELECT MAX(year) FROM publications')).scalar()
        authors      = session.execute(text('SELECT COUNT(*) FROM authors')).scalar()

        year_rows = session.execute(text("""
            SELECT year, COUNT(*) FROM publications
            WHERE year IS NOT NULL
            GROUP BY year ORDER BY year
        """)).fetchall()
        by_year = {str(y): c for y, c in year_rows}

        return {
            'total_publications':    total,
            'total_authors':         authors,
            'total_series':          series_count,
            'year_range':            {'earliest': year_min, 'latest': year_max},
            'publications_by_year':  by_year,
            'source':                'database',
            'last_updated':          datetime.utcnow().isoformat(),
        }
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Gold layer helpers
# ---------------------------------------------------------------------------

def _gold_series_stats() -> List[Dict]:
    session = _get_session()
    try:
        rows = session.execute(text("""
            SELECT series, total_publications, total_unique_authors,
                   year_min, year_max, avg_authors_per_paper, computed_at
            FROM gold.series_stats
            ORDER BY total_publications DESC
        """)).fetchall()
        return [
            {
                'series':                row[0],
                'total_publications':    row[1],
                'total_unique_authors':  row[2],
                'year_min':              row[3],
                'year_max':              row[4],
                'avg_authors_per_paper': float(row[5]) if row[5] else 0,
                'computed_at':           row[6].isoformat() if row[6] else None,
            }
            for row in rows
        ]
    finally:
        session.close()


def _gold_yearly_trends(series: str = None) -> List[Dict]:
    session = _get_session()
    try:
        if series:
            full_name = _resolve_series(series)
            rows = session.execute(text("""
                SELECT series, year, publication_count
                FROM gold.yearly_trends
                WHERE series ILIKE :s
                ORDER BY series, year
            """), {'s': f'%{full_name}%'}).fetchall()
        else:
            rows = session.execute(text("""
                SELECT series, year, publication_count
                FROM gold.yearly_trends
                ORDER BY series, year
            """)).fetchall()
        return [
            {'series': row[0], 'year': row[1], 'publication_count': row[2]}
            for row in rows
        ]
    finally:
        session.close()


def _gold_top_authors(series: str = None, limit: int = 20) -> List[Dict]:
    session = _get_session()
    try:
        if series:
            full_name = _resolve_series(series)
            rows = session.execute(text("""
                SELECT series, author_name, publication_count
                FROM gold.top_authors
                WHERE series ILIKE :s
                ORDER BY publication_count DESC
                LIMIT :lim
            """), {'s': f'%{full_name}%', 'lim': limit}).fetchall()
        else:
            rows = session.execute(text("""
                SELECT series, author_name, publication_count
                FROM gold.top_authors
                ORDER BY series, publication_count DESC
            """)).fetchall()
        return [
            {'series': row[0], 'author_name': row[1], 'publication_count': row[2]}
            for row in rows
        ]
    finally:
        session.close()


# ---------------------------------------------------------------------------
# File-based fallback (local development without DATABASE_URL)
# ---------------------------------------------------------------------------

def _load_live_files() -> Dict[str, List[Dict]]:
    live_dir = DATA_DIR / "live"
    series_data = {}
    if not live_dir.exists():
        return {}
    for file in live_dir.glob("*_filtered.json"):
        series_abbr = file.stem.replace('_filtered', '')
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            series_data[series_abbr] = data.get('publications', [])
        except Exception as e:
            print(f"Error loading {file}: {e}")
    return series_data


def _search_files(series: str = None, year: int = None,
                  query: str = None, limit: int = 100) -> List[Dict]:
    all_data = _load_live_files()
    results = []
    if series:
        results = all_data.get(series.upper(), [])
    else:
        for pubs in all_data.values():
            results.extend(pubs)
    if year:
        results = [p for p in results if p.get('year') == year]
    if query:
        q_lower = query.lower()
        results = [p for p in results if q_lower in p.get('title', '').lower()]
    results.sort(key=lambda p: p.get('year', 0), reverse=True)
    return results[:limit]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route('/')
def home():
    return jsonify({
        'name':    'Springer Publications API',
        'version': '2.0',
        'backend': 'database' if _db_available() else 'local-files',
        'endpoints': {
            '/api/series':              'All series with publication counts',
            '/api/publications':        'Search publications (params: series, year, query, limit)',
            '/api/publications/recent': 'Most recent publications',
            '/api/stats':               'Overall database statistics',
            '/api/gold/stats':          'Gold layer: per-series aggregations',
            '/api/gold/trends':         'Gold layer: yearly publication trends (param: series)',
            '/api/gold/authors':        'Gold layer: top authors (params: series, limit)',
            '/health':                  'Health check',
        },
    })


@app.route('/health')
def health():
    if _db_available():
        try:
            session = _get_session()
            session.execute(text('SELECT 1'))
            session.close()
            return jsonify({'status': 'healthy', 'database': 'connected'})
        except Exception as e:
            return jsonify({'status': 'unhealthy', 'database': str(e)}), 503
    return jsonify({'status': 'healthy', 'database': 'not configured (using local files)'})


@app.route('/api/series')
def get_series():
    if _db_available():
        try:
            return jsonify({
                'series':       _series_stats_db(),
                'source':       'database',
            })
        except Exception as e:
            print(f"DB error: {e}")

    data = _load_live_files()
    return jsonify({
        'series': [
            {
                'name':               abbr,
                'abbreviation':       abbr,
                'total_publications': len(pubs),
                'years':              sorted({p.get('year') for p in pubs if p.get('year')}),
            }
            for abbr, pubs in data.items()
        ],
        'source': 'local-files',
    })


@app.route('/api/publications')
def search_publications():
    series = request.args.get('series')
    year   = request.args.get('year', type=int)
    query  = request.args.get('query')
    limit  = min(request.args.get('limit', default=100, type=int), 1000)

    if _db_available():
        try:
            results = _search_db(series=series, year=year, query=query, limit=limit)
            return jsonify({
                'results': results,
                'count':   len(results),
                'filters': {'series': series, 'year': year, 'query': query},
                'source':  'database',
            })
        except Exception as e:
            print(f"DB error: {e}")

    results = _search_files(series=series, year=year, query=query, limit=limit)
    return jsonify({
        'results': results,
        'count':   len(results),
        'filters': {'series': series, 'year': year, 'query': query},
        'source':  'local-files',
    })


@app.route('/api/publications/recent')
def get_recent_publications():
    limit = min(request.args.get('limit', default=50, type=int), 1000)
    if _db_available():
        try:
            results = _search_db(limit=limit)
            return jsonify({'results': results, 'count': len(results), 'source': 'database'})
        except Exception as e:
            print(f"DB error: {e}")

    results = _search_files(limit=limit)
    return jsonify({'results': results, 'count': len(results), 'source': 'local-files'})


@app.route('/api/stats')
def get_stats():
    if _db_available():
        try:
            return jsonify(_stats_db())
        except Exception as e:
            print(f"DB error: {e}")

    data    = _load_live_files()
    total   = sum(len(pubs) for pubs in data.values())
    by_year: Dict[str, int] = {}
    for pubs in data.values():
        for pub in pubs:
            y = pub.get('year')
            if y:
                by_year[str(y)] = by_year.get(str(y), 0) + 1

    return jsonify({
        'total_publications': total,
        'total_series':       len(data),
        'year_range': {
            'earliest': min(int(y) for y in by_year) if by_year else None,
            'latest':   max(int(y) for y in by_year) if by_year else None,
        },
        'publications_by_year': dict(sorted(by_year.items())),
        'source':       'local-files',
        'last_updated': datetime.utcnow().isoformat(),
    })


# ---------------------------------------------------------------------------
# Gold layer endpoints
# ---------------------------------------------------------------------------

@app.route('/api/gold/stats')
def gold_stats():
    """Per-series aggregations from gold.series_stats."""
    if not _db_available():
        return jsonify({'error': 'Database not configured'}), 503
    try:
        return jsonify({'stats': _gold_series_stats(), 'source': 'gold'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/gold/trends')
def gold_trends():
    """
    Yearly publication counts from gold.yearly_trends.
    Optional param: series (abbreviation or full name)
    """
    if not _db_available():
        return jsonify({'error': 'Database not configured'}), 503
    series = request.args.get('series')
    try:
        trends = _gold_yearly_trends(series=series)
        return jsonify({'trends': trends, 'count': len(trends), 'source': 'gold'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/quality')
def quality_metrics():
    """Per-series data quality metrics from quality_metrics table."""
    if not _db_available():
        return jsonify({'error': 'Database not configured'}), 503
    try:
        session = _get_session()
        rows = session.execute(text("""
            SELECT series, total_records,
                   doi_coverage_pct, event_date_coverage_pct,
                   event_year_coverage_pct, author_coverage_pct,
                   validated_at
            FROM quality_metrics
            ORDER BY total_records DESC
        """)).fetchall()
        session.close()
        return jsonify({
            'metrics': [
                {
                    'series':                   row[0],
                    'total_records':            row[1],
                    'doi_coverage_pct':         float(row[2]) if row[2] else 0,
                    'event_date_coverage_pct':  float(row[3]) if row[3] else 0,
                    'event_year_coverage_pct':  float(row[4]) if row[4] else 0,
                    'author_coverage_pct':      float(row[5]) if row[5] else 0,
                    'validated_at':             row[6].isoformat() if row[6] else None,
                }
                for row in rows
            ],
            'source': 'quality_metrics',
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/gold/authors')
def gold_authors():
    """
    Top authors from gold.top_authors.
    Optional params: series (abbreviation or full name), limit (default 20)
    """
    if not _db_available():
        return jsonify({'error': 'Database not configured'}), 503
    series = request.args.get('series')
    limit  = min(request.args.get('limit', default=20, type=int), 200)
    try:
        authors = _gold_top_authors(series=series, limit=limit)
        return jsonify({'authors': authors, 'count': len(authors), 'source': 'gold'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    backend = 'Neon (DATABASE_URL)' if _db_available() else 'local JSON files'
    print("=" * 60)
    print("Springer Publications API")
    print(f"Backend: {backend}")
    print("Series map loaded:", list(SERIES_MAP.keys()))
    print("=" * 60)
    print("Endpoints:")
    print("  GET /health")
    print("  GET /api/series")
    print("  GET /api/publications?series=LNCS&year=2024&query=machine")
    print("  GET /api/publications/recent")
    print("  GET /api/stats")
    print("  GET /api/gold/stats")
    print("  GET /api/gold/trends?series=LNCS")
    print("  GET /api/gold/authors?series=LNCS&limit=10")
    print("=" * 60 + "\n")
    port = int(os.getenv('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
