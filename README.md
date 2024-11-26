# JetBrains Extensions Marketplace Crawler

Tool for crawling and analyzing JetBrains marketplace extensions data. This project provides functionality to collect extension information from the JetBrains Marketplace and process it into analyzable formats.

## Features

- 🔍 Crawl JetBrains Marketplace extensions data
- 📊 Export data to CSV format
- 💾 Store data in SQLite database
- 📝 Comprehensive logging
- ⚡ Efficient pagination handling
- 🛡️ Robust error handling
- 🔄 Type-safe implementation

## Prerequisites

- Python 3.8+
- pip

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/Shellomo/jetbrains-marketplace-crawler.git
    cd jetbrains-marketplace-crawler
    ```

2. Create a virtual environment (recommended):
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3. Install required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Project Structure

```
jetbrains-extensions-marketplace-crawler/
├── marketplace_crawler.py  # Main crawler implementation
├── data_processor.py       # Data processing and export functionality
├── requirements.txt        # Project dependencies
├── README.md               # Project documentation
└── logs/                   # Log files directory
    ├── marketplace_crawler.log
    └── data_processor.log
```

## Usage

### Crawling Extensions

```python
from marketplace_crawler import MarketplaceCrawler

# Initialize the crawler
crawler = MarketplaceCrawler()

# Start crawling (default: max_pages=100)
total_extensions = crawler.crawl()
```

### Processing Data

```python
from data_processor import ExtensionDataProcessor

# Initialize the processor
processor = ExtensionDataProcessor()

# Convert to CSV
processor.convert_to_csv()

# Export to SQLite (optional)
processor.export_to_sqlite()
```

### Command Line Usage

You can also run the scripts directly from the command line:

```bash
# Crawl extensions
python marketplace_crawler.py

# Process data
python data_processor.py
```

## Output Formats

### CSV Structure

The generated CSV file includes the following fields:

- `id`: Extension's unique identifier
- `name`: Extension's name
- `downloads`: Total download count
- `rating`: Extension's average rating
- `pricing`: Extension's pricing information
- `vendor`: Publisher's name
- `tags`: Extension tags
- `publishedDate`: Initial publication date
- `date`: Last update date

### SQLite Database

The data is also exported to a SQLite database with the same structure as the CSV file, making it easy to perform complex queries and analysis.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License.
