"""
SQLAlchemy ORM models — full medallion architecture in Neon PostgreSQL.

Schemas:
  bronze  — raw_responses      : raw API JSON, one row per scrape batch
  public  — publications       : silver layer (cleaned, structured)
            authors
            publication_authors
            series
            pipeline_runs
            quality_metrics
  gold    — series_stats       : per-series aggregations
            yearly_trends      : publications per year per series
            top_authors        : author publication counts

In-memory transport objects (used by the scraper before DB write):
  ScrapedAuthor, ScrapedPublication
"""

from dataclasses import dataclass, field, asdict as _dc_asdict
from typing import List, Optional as Opt
from datetime import datetime


# ---------------------------------------------------------------------------
# In-memory transport dataclasses (scraper -> DB writer)
# ---------------------------------------------------------------------------

@dataclass
class ScrapedAuthor:
    """Lightweight author record produced by the scraper."""
    name: str
    dblp_url: Opt[str] = None

    def to_dict(self):
        return _dc_asdict(self)


@dataclass
class ScrapedPublication:
    """Lightweight publication record produced by the scraper."""
    title: str
    authors: List[ScrapedAuthor]
    year: Opt[int] = None
    venue: Opt[str] = None
    series: Opt[str] = None
    volume: Opt[str] = None
    pages: Opt[str] = None
    doi: Opt[str] = None
    ee: Opt[str] = None
    isbn: Opt[str] = None
    publisher: str = "Springer"
    dblp_key: Opt[str] = None
    dblp_url: Opt[str] = None
    event_date_start: Opt[str] = None
    event_date_end: Opt[str] = None
    event_year: Opt[int] = None
    event_month: Opt[int] = None
    event_date_confidence: Opt[str] = None
    fetched_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self):
        data = _dc_asdict(self)
        data['authors'] = [a.to_dict() for a in self.authors]
        return data

    def __str__(self):
        names = ", ".join(a.name for a in self.authors[:3])
        if len(self.authors) > 3:
            names += " et al."
        return f"{self.title} - {names} ({self.year})"
from sqlalchemy import (
    Column, Integer, String, Text, Date, DateTime,
    ForeignKey, Table, Numeric, JSON, event,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.schema import DDL

Base = declarative_base()

# ---------------------------------------------------------------------------
# Schema creation — fires before tables are created
# ---------------------------------------------------------------------------
event.listen(
    Base.metadata,
    'before_create',
    DDL('CREATE SCHEMA IF NOT EXISTS bronze'),
)
event.listen(
    Base.metadata,
    'before_create',
    DDL('CREATE SCHEMA IF NOT EXISTS gold'),
)


# ---------------------------------------------------------------------------
# BRONZE — raw API responses stored as JSON
# ---------------------------------------------------------------------------

class BronzeRawResponse(Base):
    """
    One row per scrape batch.
    Stores the full raw CrossRef API response as JSON so you can always
    re-process from the original source data.
    """
    __tablename__ = 'raw_responses'
    __table_args__ = {'schema': 'bronze'}

    id            = Column(Integer, primary_key=True, autoincrement=True)
    series_abbr   = Column(String(20),  nullable=False, index=True)
    series_name   = Column(String(200))
    fetched_at    = Column(DateTime, default=datetime.utcnow, index=True)
    record_count  = Column(Integer)
    raw_data      = Column(JSON, nullable=False)   # list of raw CrossRef items


# ---------------------------------------------------------------------------
# SILVER — cleaned, structured publication data (public schema)
# ---------------------------------------------------------------------------

# Many-to-many: publications ↔ authors
publication_authors = Table(
    'publication_authors',
    Base.metadata,
    Column('publication_id', Integer, ForeignKey('publications.id', ondelete='CASCADE'), primary_key=True),
    Column('author_id',      Integer, ForeignKey('authors.id',      ondelete='CASCADE'), primary_key=True),
    Column('author_order',   Integer),
)


class Publication(Base):
    __tablename__ = 'publications'

    id        = Column(Integer, primary_key=True, autoincrement=True)
    doi       = Column(String(255), unique=True, nullable=False, index=True)
    title     = Column(Text, nullable=False)
    year      = Column(Integer, index=True)
    series    = Column(String(100), index=True)
    volume    = Column(String(20))
    pages     = Column(String(50))
    isbn      = Column(String(50))
    publisher = Column(String(100), default='Springer')
    url       = Column(Text)

    event_date_start      = Column(Date)
    event_date_end        = Column(Date)
    event_year            = Column(Integer, index=True)
    event_month           = Column(Integer)
    event_date_confidence = Column(String(20))   # high / medium / low

    fetched_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    authors = relationship('Author', secondary=publication_authors, back_populates='publications')

    def to_dict(self):
        return {
            'id':                    self.id,
            'doi':                   self.doi,
            'title':                 self.title,
            'year':                  self.year,
            'series':                self.series,
            'volume':                self.volume,
            'pages':                 self.pages,
            'isbn':                  self.isbn,
            'publisher':             self.publisher,
            'url':                   self.url,
            'event_date_start':      self.event_date_start.isoformat() if self.event_date_start else None,
            'event_date_end':        self.event_date_end.isoformat()   if self.event_date_end   else None,
            'event_year':            self.event_year,
            'event_month':           self.event_month,
            'event_date_confidence': self.event_date_confidence,
            'authors':               [{'name': a.name} for a in self.authors],
            'fetched_at':            self.fetched_at.isoformat() if self.fetched_at else None,
        }


class Author(Base):
    __tablename__ = 'authors'

    id              = Column(Integer, primary_key=True, autoincrement=True)
    name            = Column(String(255), nullable=False, index=True)
    normalized_name = Column(String(255), index=True)

    publications = relationship('Publication', secondary=publication_authors, back_populates='authors')


class Series(Base):
    __tablename__ = 'series'

    abbreviation       = Column(String(50), primary_key=True)
    full_name          = Column(String(255), nullable=False)
    issn               = Column(String(20))
    issn_electronic    = Column(String(20))
    springer_series_id = Column(String(50))
    springer_url       = Column(Text)
    description        = Column(Text)

    def to_dict(self):
        return {
            'abbreviation':    self.abbreviation,
            'full_name':       self.full_name,
            'issn':            self.issn,
            'issn_electronic': self.issn_electronic,
            'springer_url':    self.springer_url,
        }


class QualityMetric(Base):
    __tablename__ = 'quality_metrics'

    id                      = Column(Integer, primary_key=True, autoincrement=True)
    series                  = Column(String(200), index=True)
    total_records           = Column(Integer)
    doi_coverage_pct        = Column(Numeric(5, 2))
    event_date_coverage_pct = Column(Numeric(5, 2))  # has event_date_start (day-level)
    event_year_coverage_pct = Column(Numeric(5, 2))  # has event_year (from published-print)
    author_coverage_pct     = Column(Numeric(5, 2))
    validated_at            = Column(DateTime, default=datetime.utcnow, index=True)


class PipelineRun(Base):
    __tablename__ = 'pipeline_runs'

    id              = Column(Integer, primary_key=True, autoincrement=True)
    run_type        = Column(String(50))   # full / incremental / manual
    series          = Column(String(255))
    records_fetched = Column(Integer)
    records_new     = Column(Integer)
    status          = Column(String(20), index=True)   # success / failed
    error_message   = Column(Text)
    started_at      = Column(DateTime, index=True)
    completed_at    = Column(DateTime)

    def to_dict(self):
        return {
            'id':              self.id,
            'run_type':        self.run_type,
            'series':          self.series,
            'records_fetched': self.records_fetched,
            'records_new':     self.records_new,
            'status':          self.status,
            'started_at':      self.started_at.isoformat()   if self.started_at   else None,
            'completed_at':    self.completed_at.isoformat() if self.completed_at else None,
        }


# ---------------------------------------------------------------------------
# GOLD — pre-computed aggregations (analytics-ready)
# ---------------------------------------------------------------------------

class GoldSeriesStat(Base):
    """Per-series summary — recomputed after every scrape."""
    __tablename__ = 'series_stats'
    __table_args__ = {'schema': 'gold'}

    series                = Column(String(100), primary_key=True)
    total_publications    = Column(Integer)
    total_unique_authors  = Column(Integer)
    year_min              = Column(Integer)
    year_max              = Column(Integer)
    avg_authors_per_paper = Column(Numeric(5, 2))
    computed_at           = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'series':                self.series,
            'total_publications':    self.total_publications,
            'total_unique_authors':  self.total_unique_authors,
            'year_min':              self.year_min,
            'year_max':              self.year_max,
            'avg_authors_per_paper': float(self.avg_authors_per_paper) if self.avg_authors_per_paper else 0,
            'computed_at':           self.computed_at.isoformat() if self.computed_at else None,
        }


class GoldYearlyTrend(Base):
    """Publications per year per series — powers trend charts."""
    __tablename__ = 'yearly_trends'
    __table_args__ = {'schema': 'gold'}

    series            = Column(String(100), primary_key=True)
    year              = Column(Integer,     primary_key=True)
    publication_count = Column(Integer)
    computed_at       = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'series':            self.series,
            'year':              self.year,
            'publication_count': self.publication_count,
        }


class GoldTopAuthor(Base):
    """Top authors by publication count per series."""
    __tablename__ = 'top_authors'
    __table_args__ = {'schema': 'gold'}

    id                = Column(Integer, primary_key=True, autoincrement=True)
    series            = Column(String(100), index=True)
    author_name       = Column(String(255))
    publication_count = Column(Integer)
    computed_at       = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'series':            self.series,
            'author_name':       self.author_name,
            'publication_count': self.publication_count,
        }
