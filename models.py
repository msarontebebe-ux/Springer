"""Data models for publication metadata."""

from dataclasses import dataclass, field, asdict
from typing import List, Optional
from datetime import datetime


@dataclass
class Author:
    """Represents an author of a publication."""
    name: str
    dblp_url: Optional[str] = None

    def to_dict(self):
        return asdict(self)


@dataclass
class Publication:
    """Represents a publication with its metadata."""
    title: str
    authors: List[Author]
    year: Optional[int] = None
    venue: Optional[str] = None
    series: Optional[str] = None
    volume: Optional[str] = None
    pages: Optional[str] = None
    doi: Optional[str] = None
    ee: Optional[str] = None  # Electronic Edition (URL)
    isbn: Optional[str] = None
    publisher: str = "Springer"
    dblp_key: Optional[str] = None
    dblp_url: Optional[str] = None
    fetched_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self):
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        data['authors'] = [author.to_dict() for author in self.authors]
        return data

    def __str__(self):
        authors_str = ", ".join([a.name for a in self.authors[:3]])
        if len(self.authors) > 3:
            authors_str += " et al."
        return f"{self.title} - {authors_str} ({self.year})"
