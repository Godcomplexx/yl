from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict

class ScraperStrategy(ABC):
    """Abstract base class for a scraper strategy."""

    @abstractmethod
    def search_and_download(self, keyword: str, config: dict, download_dir: Path) -> List[Dict]:
        """
        Searches for videos based on a keyword, downloads them, and returns metadata.

        Args:
            keyword (str): The search term.
            config (dict): The main configuration dictionary.
            download_dir (Path): The directory to save raw videos.

        Returns:
            List[Dict]: A list of dictionaries, each containing metadata for a downloaded video.
        """
        pass
