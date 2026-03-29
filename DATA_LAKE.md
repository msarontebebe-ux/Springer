# Data Lake Architecture

## 📁 Structure

```
data/
├── bronze/          # Raw layer - Untouched API responses
│   ├── LNCS_<timestamp>_raw.json
│   ├── CCIS_<timestamp>_raw.json
│   └── ...
├── silver/          # Cleaned layer - Structured and cleaned data
│   ├── LNCS_cleaned.json
│   ├── LNCS_cleaned.csv
│   ├── CCIS_cleaned.json
│   ├── CCIS_cleaned.csv
│   └── ...
├── live/            # Recent publications (2025-2026) - For user display
│   ├── LNCS_filtered.json (2,869 records)
│   ├── LNCS_filtered.csv
│   ├── CCIS_filtered.json (3,446 records)
│   └── ... (13,040 total publications)
├── archive/         # Historical data (2020-2024) - For research/analysis
│   ├── LNCS_filtered.json (7,131 records)
│   ├── LNCS_filtered.csv
│   ├── CCIS_filtered.json (6,554 records)
│   └── ... (33,046 total publications)
└── gold/            # Analytics layer - Aggregated metrics (future)
    ├── publication_trends.json
    ├── author_statistics.json
    └── ...
```

## Layers

### 🥉 Bronze Layer (Raw)
**Purpose:** Store original API responses without any transformation

**Format:** JSON
- Complete CrossRef API responses
- All fields preserved
- No cleaning or processing
- Includes metadata: source, timestamp, record count
- Used for audit trails and reprocessing

**Example:** `bronze/LNCS_20260311_153045_raw.json`

### 🥈 Silver Layer (Cleaned)
**Purpose:** Cleaned, structured, and standardized data ready for analysis

**Format:** JSON + CSV
- Cleaned text (no encoding artifacts)
- Standardized field names
- Selected relevant fields
- Multiple formats for different use cases

**Files:**
- `silver/LNCS_cleaned.json` - For programmatic access
- `silver/LNCS_cleaned.csv` - For Excel/BI tools

**Columns:**
- title - Paper title (cleaned)
- authors - Semicolon-separated list
- year - Publication year
- series - Series name (LNCS, CCIS, etc.)
- volume - Volume number
- pages - Page range
- doi - Digital Object Identifier
- url - Direct link to paper
- isbn - ISBN of proceedings volume
- publisher - Publisher name

### 📱 Live Layer (Recent Publications)
**Purpose:** Recent publications for user-facing display

**Format:** JSON + CSV
- Filtered to recent years (2025-2026)
- Lightweight dataset for quick loading
- Updated regularly (weekly/monthly)
- Optimized for end-user applications

**Use cases:**
- "What's New" publication feed
- Recent conference proceedings display
- User-facing web applications
- Email notifications for new publications

**Current data:** 13,040 publications (2025-2026)

### 📚 Archive Layer (Historical Data)
**Purpose:** Historical publications for research and analysis

**Format:** JSON + CSV
- Contains older publications (2020-2024)
- Used for thesis research and trend analysis
- Not displayed to end users
- Reprocessed from original datasets

**Use cases:**
- Thesis data analysis
- Publication trend studies
- Historical research queries
- Comparative analysis

**Current data:** 33,046 publications (2020-2024)

### 🥇 Gold Layer (Analytics)
**Purpose:** Business-ready aggregated insights and metrics

**Future implementations:**
- Publication trends by year/series
- Author collaboration networks
- Top publishers/conferences
- Geographic distribution
- Citation metrics
- Subject area analysis

## Data Flow

```
CrossRef API
    ↓
1. Fetch → Raw JSON responses
    ↓
2. Save to Bronze → Untouched data
    ↓
3. Parse & Clean → Remove artifacts, standardize
    ↓
4. Save to Silver → JSON + CSV formats
    ↓
5. Aggregate → Calculate metrics (future)
    ↓
6. Save to Gold → Analytics ready
```

## Usage

### Filtering Publications by Year

Use the `filter_publications.py` utility to separate data by year ranges:

```bash
# Filter recent publications (2025-2026) for user display
python filter_publications.py --output-dir data/live --year-start 2025

# Filter historical data (2020-2024) for research
python filter_publications.py --output-dir data/archive --year-start 2020 --year-end 2024

# Specific series only
python filter_publications.py --output-dir data/live --year-start 2025 --series LNCS CCIS

# Custom year range
python filter_publications.py --output-dir data/custom --year-start 2023 --year-end 2024
```

### Accessing Bronze (Raw)
```python
import json

with open('data/bronze/LNCS_<timestamp>_raw.json') as f:
    data = json.load(f)
    
raw_records = data['raw_records']
# Access exactly as returned by CrossRef API
```

### Accessing Silver (Cleaned)
```python
# CSV - use pandas
import pandas as pd
df = pd.read_csv('data/silver/LNCS_cleaned.csv')

# JSON - use json
import json
with open('data/silver/LNCS_cleaned.json') as f:
    data = json.load(f)
    publications = data['publications']
```

### Accessing Live (Recent Publications)
```python
# For user display - Recent publications only (2025-2026)
import pandas as pd
df = pd.read_csv('data/live/LNCS_filtered.csv')

# Show newest publications
recent = df[df['year'] >= 2025].sort_values('year', ascending=False)
print(f"Found {len(recent)} recent publications")
```

### Accessing Archive (Historical Data)
```python
# For research/thesis - Historical data (2020-2024)
import pandas as pd
df = pd.read_csv('data/archive/LNCS_filtered.csv')

# Analyze trends over time
trends = df.groupby('year').size()
print(trends)
```

### Future Gold Analytics
```python
# Example future implementation
with open('data/gold/publication_trends.json') as f:
    trends = json.load(f)
    # {2020: 1234, 2021: 1456, ...}
```

## Benefits

✅ **Traceability** - Can always go back to raw data
✅ **Reproducibility** - Reprocess from bronze if needed
✅ **Flexibility** - Different formats for different tools
✅ **Audit Trail** - Know exactly when and what was fetched
✅ **Scalability** - Add gold analytics without re-fetching
✅ **Data Quality** - Bronze shows original, silver shows cleaned

## Metadata

Each layer includes metadata:

**Bronze:**
- source: "CrossRef API"
- fetched_at: ISO timestamp
- record_count: Number of records
- processing: "none - raw API response"

**Silver:**
- source: "CrossRef"
- processed_at: ISO timestamp
- total_count: Number of publications
- processing: "cleaned and structured"

## Best Practices

1. **Never modify Bronze** - It's the source of truth
2. **Silver can be regenerated** - From bronze if needed
3. **Document transformations** - What was cleaned/changed
4. **Version your schemas** - Track structure changes
5. **Backup Bronze** - Small files, high value

## For Your Thesis

This architecture supports both **research** and **user applications**:

### Research/Analysis (Archive Layer)
- ✅ **33,046 historical publications** (2020-2024)
- ✅ Trend analysis and statistics
- ✅ Publication patterns over time
- ✅ Author collaboration networks
- ✅ Cross-series comparisons

### User Display (Live Layer)
- ✅ **13,040 recent publications** (2025-2026)
- ✅ Fast loading for web applications
- ✅ "What's New" feeds
- ✅ Regular updates (weekly/monthly)
- ✅ Optimized for end-user experience

### Data Quality & Compliance
- ✅ Data provenance (where did data come from?)
- ✅ Data lineage (how was it transformed?)
- ✅ Reproducibility (can redo analysis)
- ✅ Compliance (audit trails)
- ✅ Quality assurance (compare raw vs cleaned)

---

## 📊 Current Dataset Summary

| Layer | Purpose | Records | Time Range |
|-------|---------|---------|------------|
| **Bronze** | Raw API responses | All | 2020-2026 |
| **Silver** | Cleaned data | 10 per series | Sample |
| **Live** | User display | 13,040 | 2025-2026 |
| **Archive** | Research | 33,046 | 2020-2024 |
| **Gold** | Analytics | 0 | Future |

**Total collected:** 46,086+ publications across 5 major series
