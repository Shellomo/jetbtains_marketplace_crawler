# data_processor.py
"""
Simple JetBrains Plugins Data Processor
Converts crawled plugin data to CSV format.
"""

import json
import csv
import logging
from pathlib import Path
from datetime import datetime
import sqlite3


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PluginDataProcessor:
    """Process and convert JetBrains plugin data to CSV format."""

    def __init__(self, plugins_dir: str = 'plugins'):
        self.plugins_dir = Path(plugins_dir)
        # Define the fields we want to extract
        self.fields = {
            'id': 'id',
            'name': 'name',
            'downloads': 'downloads',
            'rating': 'rating',
            'pricing': 'pricingModel',
            'vendor': 'vendor_name',
            'tags': 'tags',
            'date': 'cdate'
        }

    def process_plugins(self, output_file: str = 'plugins.csv') -> None:
        """Load JSON files and convert to CSV."""
        all_plugins = []

        # Load all JSON files
        try:
            for file_path in self.plugins_dir.glob('*.json'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                    plugins_data = json_data.get('plugins', [])
                    all_plugins.extend(plugins_data)
            logger.info(f"Loaded {len(all_plugins)} plugins from {len(list(self.plugins_dir.glob('*.json')))} files")
        except Exception as e:
            logger.error(f"Error loading plugins: {e}")
            return

        # Prepare data for CSV
        processed_data = []
        for plugin in all_plugins:
            row = {}
            for field_name, json_path in self.fields.items():
                if '_' in json_path:  # Handle nested fields (vendor_name)
                    parent, child = json_path.split('_')
                    value = plugin.get(parent, {}).get(child, '')
                else:
                    value = plugin.get(json_path, '')

                # Special handling for specific fields
                if field_name == 'tags':
                    value = ','.join(value) if value else ''
                elif field_name == 'date':
                    value = datetime.fromtimestamp(value / 1000).strftime('%Y-%m-%d') if value else ''

                row[field_name] = value
            processed_data.append(row)

        # Write to CSV
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fields.keys())
                writer.writeheader()
                writer.writerows(processed_data)
            logger.info(f"Successfully wrote {len(processed_data)} plugins to {output_file}")
        except Exception as e:
            logger.error(f"Error writing CSV: {e}")

        # drop to sqlite
        conn = sqlite3.connect('plugins.db')
        c = conn.cursor()
        c.execute('CREATE TABLE plugins (id, name, downloads, rating, pricing, vendor, tags, date)')
        for plugin in processed_data:
            c.execute('INSERT INTO plugins VALUES (?, ?, ?, ?, ?, ?, ?, ?)', (plugin['id'], plugin['name'], plugin['downloads'], plugin['rating'], plugin['pricing'], plugin['vendor'], plugin['tags'], plugin['date']))
        conn.commit()
        conn.close()
        logger.info(f"Successfully wrote {len(processed_data)} plugins to plugins.db")


def main():
    """Main entry point."""
    processor = PluginDataProcessor()
    processor.process_plugins()


if __name__ == '__main__':
    main()