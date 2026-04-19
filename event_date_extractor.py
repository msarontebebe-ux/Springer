"""
Extract conference event dates from Springer proceedings metadata.

CrossRef book-level records almost never include an explicit event field.
The main extraction path is title parsing, which yields results for the
small subset of titles that embed conference dates (e.g.
"ICAPR 2001, March 28-30, 2001, Calcutta, India").
"""

import re
from datetime import datetime, date
from typing import Optional, Dict, Any


def _valid_date(year: int, month: int, day: int) -> bool:
    try:
        date(year, month, day)
        return True
    except ValueError:
        return False


class ConferenceDateExtractor:
    """Extract conference event dates from publication metadata."""

    MONTH_MAP = {
        'january': 1,  'jan': 1,
        'february': 2, 'feb': 2,
        'march': 3,    'mar': 3,
        'april': 4,    'apr': 4,
        'may': 5,
        'june': 6,     'jun': 6,
        'july': 7,     'jul': 7,
        'august': 8,   'aug': 8,
        'september': 9,'sep': 9, 'sept': 9,
        'october': 10, 'oct': 10,
        'november': 11,'nov': 11,
        'december': 12,'dec': 12,
    }

    _MONTH_RE = (
        r'january|february|march|april|may|june|july|august|'
        r'september|october|november|december|'
        r'jan|feb|mar|apr|jun|jul|aug|sept?|oct|nov|dec'
    )

    # Ordered from most to least specific — first match wins.
    # Each tuple: (compiled_regex, handler_key)
    # Hyphens and en-dashes (U+2013) are both accepted as date separators.
    _PATTERNS = [
        # "May 15-17, 2024"  or  "May 15–17, 2024"
        (re.compile(
            r'\b(' + _MONTH_RE + r')\s+(\d{1,2})[–\-](\d{1,2})[,\s]+(\d{4})\b',
            re.IGNORECASE), 'month_dd_dd_yyyy'),

        # "15-17 May 2024"  or  "15–17 May 2024"
        (re.compile(
            r'\b(\d{1,2})[–\-](\d{1,2})\s+(' + _MONTH_RE + r')[,\s]+(\d{4})\b',
            re.IGNORECASE), 'dd_dd_month_yyyy'),

        # "May 15, 2024"  or  "May 15 2024"
        (re.compile(
            r'\b(' + _MONTH_RE + r')\s+(\d{1,2})[,\s]+(\d{4})\b',
            re.IGNORECASE), 'month_dd_yyyy'),

        # "May 2024"
        (re.compile(
            r'\b(' + _MONTH_RE + r')\s+(\d{4})\b',
            re.IGNORECASE), 'month_yyyy'),
    ]

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    @classmethod
    def extract_from_title(cls, title: str) -> Optional[Dict[str, Any]]:
        """
        Parse a conference date from the proceedings title string.
        Returns a dict with event date fields or None if nothing found.
        """
        if not title:
            return None

        for pattern, handler in cls._PATTERNS:
            m = pattern.search(title)
            if not m:
                continue
            g = m.groups()

            if handler == 'month_dd_dd_yyyy':
                month = cls.MONTH_MAP.get(g[0].lower())
                if not month:
                    continue
                year, d1, d2 = int(g[3]), int(g[1]), int(g[2])
                if not _valid_date(year, month, d1):
                    continue
                return {
                    'event_date_start':      f'{year}-{month:02d}-{d1:02d}',
                    'event_date_end':        f'{year}-{month:02d}-{d2:02d}' if _valid_date(year, month, d2) else None,
                    'event_year':            year,
                    'event_month':           month,
                    'confidence':            'high',
                }

            elif handler == 'dd_dd_month_yyyy':
                month = cls.MONTH_MAP.get(g[2].lower())
                if not month:
                    continue
                year, d1, d2 = int(g[3]), int(g[0]), int(g[1])
                if not _valid_date(year, month, d1):
                    continue
                return {
                    'event_date_start':      f'{year}-{month:02d}-{d1:02d}',
                    'event_date_end':        f'{year}-{month:02d}-{d2:02d}' if _valid_date(year, month, d2) else None,
                    'event_year':            year,
                    'event_month':           month,
                    'confidence':            'high',
                }

            elif handler == 'month_dd_yyyy':
                month = cls.MONTH_MAP.get(g[0].lower())
                if not month:
                    continue
                year, day = int(g[2]), int(g[1])
                if not _valid_date(year, month, day):
                    continue
                return {
                    'event_date_start':      f'{year}-{month:02d}-{day:02d}',
                    'event_date_end':        None,
                    'event_year':            year,
                    'event_month':           month,
                    'confidence':            'high',
                }

            elif handler == 'month_yyyy':
                month = cls.MONTH_MAP.get(g[0].lower())
                if not month:
                    continue
                year = int(g[1])
                return {
                    'event_date_start':      f'{year}-{month:02d}-01',
                    'event_date_end':        None,
                    'event_year':            year,
                    'event_month':           month,
                    'confidence':            'medium',
                }

        return None

    @classmethod
    def extract_from_crossref(cls, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extract event date from a raw CrossRef API item.
        CrossRef book records rarely carry an explicit event field, so this
        falls back to the published-print year (confidence=low).
        """
        event = item.get('event')
        if event:
            start = event.get('start')
            end   = event.get('end')
            if start:
                parts = start.split('-')
                return {
                    'event_date_start': start,
                    'event_date_end':   end,
                    'event_year':       int(parts[0]) if parts else None,
                    'event_month':      int(parts[1]) if len(parts) > 1 else None,
                    'confidence':       'high',
                }

        # Fallback: use the publication year as an approximation
        published = item.get('published-print') or item.get('published-online')
        if published:
            date_parts = published.get('date-parts', [[]])[0]
            if date_parts:
                year  = date_parts[0] if len(date_parts) > 0 else None
                month = date_parts[1] if len(date_parts) > 1 else None
                return {
                    'event_date_start': None,
                    'event_date_end':   None,
                    'event_year':       year,
                    'event_month':      month,
                    'confidence':       'low',
                }

        return None
