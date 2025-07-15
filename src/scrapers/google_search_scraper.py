import logging
from pathlib import Path
from typing import List, Dict
import http.client
import json
import time
import yt_dlp
import re
import urllib.parse

from .base_scraper import ScraperStrategy

log = logging.getLogger('CinematicDatasetCollector')

class GoogleSearchScraper(ScraperStrategy):
    """
    Scraper strategy that uses Google Search API via RapidAPI to find videos across multiple platforms,
    and then uses yt-dlp to download them.
    """

    def search_and_download(self, keyword: str, config: dict, download_dir: Path) -> List[Dict]:
        """
        Uses Google Search API to find videos matching the keyword, then downloads them using yt-dlp.
        
        Args:
            keyword (str): The search term.
            config (dict): The main configuration dictionary.
            download_dir (Path): The directory to save raw videos.
            
        Returns:
            List[Dict]: A list of dictionaries, each containing metadata for a downloaded video.
        """
        scraper_config = config['scraper']
        limit_per_keyword = scraper_config['download_limit_per_keyword']
        max_duration = scraper_config['max_video_duration']
        api_key = config.get('rapidapi', {}).get('key', '')
        
        if not api_key:
            log.error("[GoogleSearch] RapidAPI key not found in configuration. Skipping search.")
            return []
        
        downloaded_videos = []
        
        # Формируем поисковый запрос для видео
        cinematic_query = f"{keyword} cinematic video"
        log.info(f"[GoogleSearch] Phase 1: Searching for '{cinematic_query}'")
        
        try:
            # Подключаемся к Google Search API через RapidAPI
            conn = http.client.HTTPSConnection("google-search74.p.rapidapi.com")
            
            headers = {
                'x-rapidapi-key': api_key,
                'x-rapidapi-host': "google-search74.p.rapidapi.com"
            }
            
            # Кодируем запрос для URL
            encoded_query = urllib.parse.quote(cinematic_query)
            conn.request("GET", f"/?query={encoded_query}&limit=20&related_keywords=true", headers=headers)
            
            res = conn.getresponse()
            data = res.read()
            
            if res.status != 200:
                log.error(f"[GoogleSearch] API request failed with status code {res.status}: {data.decode('utf-8')}")
                return []
            
            search_results = json.loads(data.decode("utf-8"))
            
            # Извлекаем URL видео из результатов поиска
            video_urls = []
            
            # Проверяем наличие результатов
            if 'results' not in search_results:
                log.warning(f"[GoogleSearch] No results found for '{cinematic_query}'")
                return []
            
            # Ищем URL видео в результатах поиска
            for result in search_results['results']:
                url = result.get('url', '')
                
                # Проверяем, является ли URL видео с поддерживаемой платформы
                if any(platform in url for platform in ['youtube.com/watch', 'youtu.be', 'tiktok.com', 'vimeo.com', 'instagram.com']):
                    video_urls.append(url)
            
            log.info(f"[GoogleSearch] Found {len(video_urls)} video URLs for '{keyword}'")
            
            # Ограничиваем количество видео для скачивания
            video_urls = video_urls[:limit_per_keyword]
            
            if not video_urls:
                return []
            
            # Скачиваем найденные видео с помощью yt-dlp
            log.info(f"[GoogleSearch] Phase 2: Downloading {len(video_urls)} videos for '{keyword}'")
            
            for video_url in video_urls:
                try:
                    # Сначала получаем метаданные для проверки длительности
                    with yt_dlp.YoutubeDL({'quiet': True, 'ignoreerrors': True}) as ydl:
                        info = ydl.extract_info(video_url, download=False)
                        
                        if not info:
                            log.warning(f"[GoogleSearch] Could not extract info for {video_url}")
                            continue
                        
                        # Проверяем длительность
                        duration = info.get('duration', 0)
                        if duration > max_duration:
                            log.info(f"[GoogleSearch] Skipping {video_url} - too long ({duration}s > {max_duration}s)")
                            continue
                        
                        # Скачиваем видео
                        log.info(f"[GoogleSearch] Downloading: {video_url}")
                        
                        download_ydl_opts = {
                            'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                            'outtmpl': str(download_dir / '%(id)s.%(ext)s'),
                            'quiet': True,
                            'ignoreerrors': True,
                            'socket_timeout': 30,
                        }
                        
                        with yt_dlp.YoutubeDL(download_ydl_opts) as download_ydl:
                            info_dict = download_ydl.extract_info(video_url, download=True)
                            filepath_str = download_ydl.prepare_filename(info_dict)
                            
                            # Определяем источник видео
                            source = 'unknown'
                            if 'youtube.com' in video_url or 'youtu.be' in video_url:
                                source = 'youtube'
                            elif 'tiktok.com' in video_url:
                                source = 'tiktok'
                            elif 'vimeo.com' in video_url:
                                source = 'vimeo'
                            elif 'instagram.com' in video_url:
                                source = 'instagram'
                            
                            downloaded_videos.append({
                                'id': info_dict.get('id'),
                                'title': info_dict.get('title'),
                                'original_url': info_dict.get('webpage_url', video_url),
                                'filepath': Path(filepath_str),
                                'keyword': keyword,
                                'source': source
                            })
                            log.info(f"[GoogleSearch] Successfully downloaded: {filepath_str} from {source}")
                
                except Exception as e:
                    log.error(f"[GoogleSearch] Failed to download {video_url}. Reason: {e}")
        
        except Exception as e:
            log.error(f"[GoogleSearch] An error occurred during the process for '{keyword}': {e}")
        
        return downloaded_videos
