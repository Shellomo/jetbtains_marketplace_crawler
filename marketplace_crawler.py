# jetbrains_crawler.py
"""
JetBrains Plugin Marketplace Crawler

This module provides functionality to crawl and download plugin data
from the JetBrains Plugin Marketplace.
"""

import os
import json
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional
import requests
from requests.exceptions import RequestException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class MarketplaceConfig:
    """Configuration for JetBrains Marketplace API."""
    base_url: str = 'https://plugins.jetbrains.com/api/searchPlugins'
    headers: Dict[str, str] = None
    products: List[str] = None

    def __post_init__(self):
        if self.headers is None:
            self.headers = {
                'accept': 'application/json, text/plain',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'no-cache',
                'pragma': 'no-cache',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
            }

        if self.products is None:
            self.products = [
                'androidstudio', 'appcode', 'aqua', 'clion', 'dataspell',
                'dbe', 'fleet', 'go', 'idea', 'idea_ce', 'mps', 'phpstorm',
                'pycharm', 'pycharm_ce', 'rider', 'ruby', 'rust', 'webstorm',
                'writerside'
            ]


class JetBrainsMarketplaceCrawler:
    """Crawler for JetBrains Marketplace plugins."""

    def __init__(self, config: MarketplaceConfig = None):
        self.config = config or MarketplaceConfig()
        self._setup_output_directory()

    def _setup_output_directory(self) -> None:
        """Create output directory if it doesn't exist."""
        os.makedirs('plugins', exist_ok=True)

    def _build_url(self, offset: int, max_results: int = 100) -> str:
        """Build URL with query parameters."""
        products_param = '&'.join(f'products={p}' for p in self.config.products)
        return (
            f"{self.config.base_url}?"
            f"excludeTags=internal&"
            f"max={max_results}&"
            f"offset={offset}&"
            f"orderBy=downloads&"
            f"{products_param}"
        )

    def _make_request(self, offset: int, max_results: int = 100) -> Optional[List[Dict]]:
        """Make API request for a specific offset."""
        try:
            url = self._build_url(offset, max_results)
            response = requests.get(
                url,
                headers=self.config.headers,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            logger.error(f"Error making request for offset {offset}: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing response for offset {offset}: {str(e)}")
            return None

    def _save_plugins(self, plugins: List[Dict], page: int) -> None:
        """Save plugins data to JSON file."""
        try:
            output_path = os.path.join('plugins', f'page_{page}.json')
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(plugins, f, indent=4)
        except IOError as e:
            logger.error(f"Error saving plugins for page {page}: {str(e)}")

    def crawl(self, max_pages: int = 100, plugins_per_page: int = 100) -> int:
        """
        Crawl the JetBrains Marketplace for plugins.

        Args:
            max_pages: Maximum number of pages to crawl
            plugins_per_page: Number of plugins to fetch per request

        Returns:
            Total number of plugins crawled
        """
        total_plugins = 0

        for page in range(max_pages):
            offset = page * plugins_per_page
            plugins = self._make_request(offset, plugins_per_page)

            if not plugins:
                logger.info(f"No more plugins found after page {page}")
                break

            if isinstance(plugins, list) and not plugins:
                logger.info(f"Reached end of results at page {page}")
                break

            total_plugins += len(plugins)
            logger.info(f"Crawled page {page + 1}: Found {len(plugins)} plugins "
                        f"(Total: {total_plugins})")

            self._save_plugins(plugins, page + 1)

        return total_plugins


def main():
    """Main entry point for the crawler."""
    try:
        crawler = JetBrainsMarketplaceCrawler()
        total_plugins = crawler.crawl()
        logger.info(f"Crawling completed. Total plugins: {total_plugins}")
    except Exception as e:
        logger.error(f"Unexpected error during crawling: {str(e)}")
        raise


if __name__ == '__main__':
    main()