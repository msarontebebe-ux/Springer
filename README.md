# Springer Conference Proceedings Scraper

Fetch metadata for Springer conference proceedings published in LNCS, CCIS, and other series using the CrossRef API.

## Quick Start

```bash
# Install dependencies
python -m pip install -r requirements.txt

# Scrape LNCS conference proceedings from 2024
python main_crossref.py --series LNCS --year-start 2024

# Scrape all series from 2020
python main_crossref.py --year-start 2020
```

## What This Scrapes

**Conference proceedings** published by Springer in:
- **LNCS** - Lecture Notes in Computer Science
- **LNAI** - Lecture Notes in Artificial Intelligence  
- **LNBI** - Lecture Notes in Bioinformatics
- **CCIS** - Communications in Computer and Information Science
- **IFIP AICT** - IFIP Advances in ICT
- **LNEE** - Lecture Notes in Electrical Engineering
- **LNNS** - Lecture Notes in Networks and Systems
- **AISC** - Advances in Intelligent Systems and Computing

Each result is a **conference paper** with complete metadata (DOI, ISBN, authors, pages).

## Usage

```bash
# All series from 2020
python main_crossref.py --year-start 2020

# Specific series
python main_crossref.py --series LNCS --year-start 2024

# Year range
python main_crossref.py --series CCIS --year-start 2020 --year-end 2025

# Limit results
python main_crossref.py --series LNCS --max-results 100
```

## Output

Results saved to `data/` folder:
- `LNCS.json` / `LNCS.csv`
- `CCIS.json` / `CCIS.csv`
- `all_springer_publications_crossref.json` / `.csv` (combined)

Each publication includes:
- Title, Authors, Year
- DOI, ISBN, Pages  
- Series name, Publisher

## Project Files

```
project/
├── main_crossref.py        # Main script ⭐
├── crossref_scraper.py     # CrossRef API client
├── models.py               # Data models
├── storage.py              # JSON/CSV export
├── config.json             # Series configuration (ISSN)
├── requirements.txt        # Dependencies
└── data/                   # Output files
```

## Configuration

Edit `config.json` to:
- Modify series (all include ISSN)
- Add your email for faster API access
- Adjust delay and max results

## How It Works

Uses **CrossRef API** (free, no authentication):
- Queries by ISSN to identify each Springer series
- Filters by year
- Returns DOI, ISBN, and full metadata
- API: https://api.crossref.org

## Notes

- These are **conference proceedings** (papers published in conference volumes)
- Each DOI includes chapter number (e.g., `_22` = chapter 22)
- Best coverage from 2010+
- Free API with no rate limits if you add email to config

## Examples

```bash
# Recent LNCS proceedings
python main_crossref.py --series LNCS --year-start 2024

# All series since 2015  
python main_crossref.py --year-start 2015

# CCIS 2020-2025
python main_crossref.py --series CCIS --year-start 2020 --year-end 2025
```

## License

For academic/research use.
