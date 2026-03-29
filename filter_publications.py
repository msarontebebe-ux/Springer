"""
Filter publications by year range for live vs archive data separation.

This utility helps separate historical data (for research) from recent 
publications (for user display).
"""

import json
import argparse
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime


def load_publications(filepath: Path) -> Dict[str, Any]:
    """Load publications from JSON file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def filter_by_year_range(publications: List[Dict], start_year: int = None, 
                         end_year: int = None) -> List[Dict]:
    """
    Filter publications by year range.
    
    Args:
        publications: List of publication dictionaries
        start_year: Start year (inclusive), None for no lower bound
        end_year: End year (inclusive), None for no upper bound
    
    Returns:
        Filtered list of publications
    """
    filtered = []
    
    for pub in publications:
        year = pub.get('year')
        
        if year is None:
            continue
            
        # Apply filters
        if start_year and year < start_year:
            continue
        if end_year and year > end_year:
            continue
            
        filtered.append(pub)
    
    return filtered


def save_filtered_data(publications: List[Dict], output_path: Path, 
                       series_name: str, year_range: str):
    """Save filtered publications to JSON and CSV."""
    output_data = {
        'metadata': {
            'total_count': len(publications),
            'source': 'CrossRef',
            'series': series_name,
            'filtered_at': datetime.now().isoformat(),
            'year_range': year_range,
            'processing': 'filtered by year'
        },
        'publications': publications
    }
    
    # Save JSON
    json_path = output_path.with_suffix('.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"  ✓ Saved {len(publications)} publications to {json_path}")
    
    # Save CSV
    if publications:
        csv_path = output_path.with_suffix('.csv')
        import csv
        
        with open(csv_path, 'w', encoding='utf-8', newline='') as f:
            fieldnames = ['title', 'authors', 'year', 'series', 'volume', 
                         'pages', 'doi', 'url', 'isbn', 'publisher']
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            
            writer.writeheader()
            for pub in publications:
                # Format authors as semicolon-separated string
                if isinstance(pub.get('authors'), list):
                    authors_str = '; '.join([
                        a.get('name', '') if isinstance(a, dict) else str(a) 
                        for a in pub['authors']
                    ])
                else:
                    authors_str = str(pub.get('authors', ''))
                
                row = {
                    'title': pub.get('title', ''),
                    'authors': authors_str,
                    'year': pub.get('year', ''),
                    'series': pub.get('series', ''),
                    'volume': pub.get('volume', ''),
                    'pages': pub.get('pages', ''),
                    'doi': pub.get('doi', ''),
                    'url': pub.get('ee', ''),
                    'isbn': pub.get('isbn', ''),
                    'publisher': pub.get('publisher', '')
                }
                writer.writerow(row)
        
        print(f"  ✓ Saved CSV to {csv_path}")


def process_series(input_file: Path, output_dir: Path, series_abbr: str,
                   start_year: int = None, end_year: int = None):
    """Process a single series file."""
    print(f"\nProcessing {series_abbr}...")
    
    # Load data
    data = load_publications(input_file)
    publications = data.get('publications', [])
    
    print(f"  Total publications: {len(publications)}")
    
    # Filter by year
    filtered = filter_by_year_range(publications, start_year, end_year)
    print(f"  Filtered to: {len(filtered)} publications ({start_year or 'start'}-{end_year or 'end'})")
    
    if not filtered:
        print(f"  ⚠ No publications found in year range")
        return 0
    
    # Save
    year_range = f"{start_year or 'start'}-{end_year or 'end'}"
    output_path = output_dir / f"{series_abbr}_filtered"
    save_filtered_data(filtered, output_path, series_abbr, year_range)
    
    return len(filtered)


def main():
    parser = argparse.ArgumentParser(
        description='Filter Springer publications by year range'
    )
    parser.add_argument(
        '--input-dir',
        default='data',
        help='Input directory containing JSON files'
    )
    parser.add_argument(
        '--output-dir',
        required=True,
        help='Output directory for filtered data (e.g., data/live or data/archive)'
    )
    parser.add_argument(
        '--series',
        nargs='+',
        default=['LNCS', 'LNAI', 'LNBI', 'CCIS', 'IFIP_AICT', 'LNEE', 'LNNS', 'AISC'],
        help='Series to process'
    )
    parser.add_argument(
        '--year-start',
        type=int,
        help='Start year (inclusive)'
    )
    parser.add_argument(
        '--year-end',
        type=int,
        help='End year (inclusive)'
    )
    
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"{'='*60}")
    print(f"Filtering Publications")
    print(f"{'='*60}")
    print(f"Input: {input_dir}")
    print(f"Output: {output_dir}")
    print(f"Year range: {args.year_start or 'start'} - {args.year_end or 'end'}")
    
    total_filtered = 0
    
    for series in args.series:
        input_file = input_dir / f"{series}.json"
        
        if not input_file.exists():
            print(f"\n⚠ Skipping {series}: {input_file} not found")
            continue
        
        count = process_series(input_file, output_dir, series, 
                              args.year_start, args.year_end)
        total_filtered += count
    
    print(f"\n{'='*60}")
    print(f"✓ Completed: {total_filtered} total publications filtered")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
