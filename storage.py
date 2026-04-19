"""Storage handler for publication data with data lake architecture."""

import json
import csv
import os
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
from models_db import ScrapedPublication as Publication, ScrapedAuthor as Author


class DataLakeStorageHandler:
    """
    Handle storage with proper data lake architecture (Bronze/Silver/Gold layers).
    
    Bronze Layer: Raw API responses (untouched)
    Silver Layer: Cleaned and structured data
    Gold Layer: Aggregated analytics and metrics
    """
    
    def __init__(self, base_dir: str = "data"):
        """
        Initialize data lake storage handler.
        
        Args:
            base_dir: Base directory for data lake
        """
        self.base_dir = Path(base_dir)
        
        # Create data lake layers
        self.bronze_dir = self.base_dir / "bronze"
        self.silver_dir = self.base_dir / "silver"
        self.gold_dir = self.base_dir / "gold"
        
        # Create directories
        for directory in [self.bronze_dir, self.silver_dir, self.gold_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        
        print(f"Data Lake initialized:")
        print(f"   Bronze (raw): {self.bronze_dir}")
        print(f"   Silver (cleaned): {self.silver_dir}")
        print(f"   Gold (analytics): {self.gold_dir}")
    
    def save_bronze_raw(self, raw_data: List[Dict[Any, Any]], series_name: str, batch_id: str = None):
        """
        Save raw API responses to Bronze layer (completely untouched).
        
        Args:
            raw_data: List of raw API response items
            series_name: Name of the series
            batch_id: Optional batch identifier for incremental loads
        """
        safe_name = series_name.replace(' ', '_').replace('/', '_')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if batch_id:
            filename = f"{safe_name}_{batch_id}_raw.json"
        else:
            filename = f"{safe_name}_{timestamp}_raw.json"
        
        filepath = self.bronze_dir / filename
        
        # Save completely raw - no processing
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump({
                'metadata': {
                    'source': 'CrossRef API',
                    'series': series_name,
                    'fetched_at': datetime.now().isoformat(),
                    'record_count': len(raw_data),
                    'layer': 'bronze',
                    'processing': 'none - raw API response'
                },
                'raw_records': raw_data
            }, f, indent=2, ensure_ascii=False)
        
        print(f"   Bronze: {filename} ({len(raw_data)} raw records)")
        return str(filepath)
    
    @staticmethod
    def clean_text(text: str) -> str:
        """
        Clean text to remove encoding issues and invalid characters.
        
        Args:
            text: Text to clean
            
        Returns:
            Cleaned text
        """
        if not text or not isinstance(text, str):
            return str(text) if text else ""
        
        # Remove common encoding artifacts and special characters
        text = text.replace('\u00a0', ' ')  # Non-breaking space
        text = text.replace('\u00c2', '')   # Latin capital Â
        text = text.replace('\u200b', '')   # Zero-width space
        text = text.replace('\ufeff', '')   # BOM
        text = text.replace('\u00ad', '')   # Soft hyphen
        
        # Remove excessive dollar signs (LaTeX formatting artifacts)
        text = text.replace('$$$', '')
        text = text.replace('$$', '')
        
        # Normalize whitespace
        text = ' '.join(text.split())
        
        return text.strip()
    
    def save_silver_json(self, publications: List[Publication], series_name: str):
        """
        Save cleaned publications to Silver layer (JSON format).
        
        Args:
            publications: List of Publication objects (cleaned)
            series_name: Name of the series
        """
        safe_name = series_name.replace(' ', '_').replace('/', '_')
        filepath = self.silver_dir / f"{safe_name}_cleaned.json"
        
        data = {
            'metadata': {
                'total_count': len(publications),
                'source': 'CrossRef',
                'series': series_name,
                'processed_at': datetime.now().isoformat(),
                'layer': 'silver',
                'processing': 'cleaned and structured'
            },
            'publications': [pub.to_dict() for pub in publications]
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"   Silver: {safe_name}_cleaned.json")
        return str(filepath)
    
    def save_silver_csv(self, publications: List[Publication], series_name: str):
        """
        Save cleaned publications to Silver layer (CSV format).
        
        Args:
            publications: List of Publication objects (cleaned)
            series_name: Name of the series
        """
        safe_name = series_name.replace(' ', '_').replace('/', '_')
        filepath = self.silver_dir / f"{safe_name}_cleaned.csv"
        
        if not publications:
            print(f"   WARNING: No publications to save for {series_name}")
            return str(filepath)
        
        # Define CSV columns with clear, descriptive names
        fieldnames = [
            'title',           # Paper title
            'authors',         # Author names (semicolon-separated)
            'year',            # Publication year
            'series',          # Book series name (LNCS, CCIS, etc.)
            'volume',          # Volume number
            'pages',           # Page range (e.g., 123-145)
            'doi',             # Digital Object Identifier (unique ID)
            'url',             # Direct link to paper
            'isbn',            # ISBN of proceedings volume
            'publisher',       # Publisher name
            'event_date_start', # Conference start date
            'event_date_end',   # Conference end date
            'event_year',       # Conference year
            'event_month',      # Conference month
            'event_date_confidence'  # Date extraction confidence
        ]
        
        # Write with UTF-8 BOM to help Excel recognize encoding
        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for pub in publications:
                row = pub.to_dict()
                
                # Clean text fields
                if row.get('title'):
                    row['title'] = self.clean_text(row['title'])
                if row.get('pages'):
                    row['pages'] = self.clean_text(str(row['pages']))
                if row.get('publisher'):
                    row['publisher'] = self.clean_text(row['publisher'])
                if row.get('series'):
                    row['series'] = self.clean_text(row['series'])
                
                # Convert authors list to string
                row['authors'] = '; '.join([self.clean_text(a['name']) for a in row['authors']])
                
                # Rename 'ee' to 'url' for clarity
                row['url'] = row.get('ee', '')
                
                # Remove unnecessary fields
                row.pop('fetched_at', None)
                row.pop('dblp_key', None)
                row.pop('dblp_url', None)
                row.pop('venue', None)
                row.pop('ee', None)  # Using 'url' instead
                
                writer.writerow(row)
        
        print(f"   Silver: {safe_name}_cleaned.csv")
        return str(filepath)
    
    def save_by_series(self, publications: List[Publication], raw_data: List[Dict[Any, Any]], series_name: str):
        """
        Save publications across all data lake layers.
        
        Args:
            publications: List of Publication objects (cleaned)
            raw_data: List of raw API response items
            series_name: Name of the series
        """
        print(f"\nSaving {series_name} to Data Lake:")
        
        # Bronze: Raw API responses
        self.save_bronze_raw(raw_data, series_name)
        
        # Silver: Cleaned data
        self.save_silver_json(publications, series_name)
        self.save_silver_csv(publications, series_name)
        
        print(f"Done: {series_name} saved to all layers\n")

