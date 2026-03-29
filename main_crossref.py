"""Main script for scraping Springer publications using CrossRef API."""

import json
import argparse
from pathlib import Path
from crossref_scraper import CrossRefScraper
from storage import StorageHandler


def load_config(config_path: str = "config.json") -> dict:
    """Load configuration from JSON file."""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def scrape_single_series(series_config: dict, scraper: CrossRefScraper, 
                        storage: StorageHandler, year_start=None, year_end=None,
                        max_results=50000):
    """
    Scrape a single Springer series using CrossRef.
    
    Args:
        series_config: Series configuration dictionary
        scraper: CrossRefScraper instance
        storage: StorageHandler instance
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
        
        # Save to data lake (bronze + silver layers)
        storage.handler.save_by_series(publications, raw_data, abbr)
        print(f"✓ Successfully scraped {len(publications)} publications from {abbr}")
    else:
        print(f"⚠ No publications found for {abbr}")
    
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
    storage = StorageHandler(output_dir=output_config['directory'])
    
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
    
    # Save combined data
    if all_publications:
        storage.save_json(all_publications, "all_springer_publications_crossref")
        storage.save_csv(all_publications, "all_springer_publications_crossref")
    
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
    storage = StorageHandler(output_dir=output_config['directory'])
    
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
