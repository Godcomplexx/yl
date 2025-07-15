import logging
from pathlib import Path
from typing import List, Dict
import requests
import json
import os
import time
from urllib.parse import quote

from .base_scraper import ScraperStrategy

log = logging.getLogger('CinematicDatasetCollector')

class TikTokScraper(ScraperStrategy):
    """Scraper strategy for downloading videos from TikTok using RapidAPI."""

    def search_and_download(self, keyword: str, config: dict, download_dir: Path) -> List[Dict]:
        """
        Searches TikTok for videos using RapidAPI, filters them by duration, and downloads suitable ones.
        
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
            log.error("[TikTok] RapidAPI key not found in configuration. Skipping TikTok scraping.")
            return []
        
        downloaded_videos = []
        log.info(f"[TikTok] Phase 1: Fetching metadata for keyword: '{keyword}'")
        
        try:
            # 1. Fetch metadata using RapidAPI
            url = "https://tiktok-video-no-watermark2.p.rapidapi.com/feed/search"
            
            querystring = {
                "keywords": keyword,
                "count": "20",  # Request more videos to filter by duration later
                "cursor": "0"
            }
            
            headers = {
                "X-RapidAPI-Key": api_key,
                "X-RapidAPI-Host": "tiktok-video-no-watermark2.p.rapidapi.com"
            }
            
            response = requests.get(url, headers=headers, params=querystring)
            
            if response.status_code != 200:
                log.error(f"[TikTok] API request failed with status code {response.status_code}: {response.text}")
                return []
            
            result = response.json()
            
            if not result or 'data' not in result or not result['data']:
                log.warning(f"[TikTok] No videos found for keyword: {keyword}")
                return []
            
            # 2. Filter videos by duration
            suitable_videos = []
            for video in result['data']:
                if not video: continue
                duration = video.get('duration', 0)
                if 0 < duration <= max_duration:
                    suitable_videos.append(video)
            
            videos_to_download = suitable_videos[:limit_per_keyword]
            log.info(f"[TikTok] Found {len(videos_to_download)} suitable videos (under {max_duration}s) for '{keyword}'.")
            
            # 3. Download the filtered videos
            if not videos_to_download:
                return []
            
            log.info(f"[TikTok] Phase 2: Downloading {len(videos_to_download)} videos for '{keyword}'.")
            
            for video_info in videos_to_download:
                video_id = video_info.get('id')
                video_url = video_info.get('play')  # No watermark URL from the API
                
                if not video_url or not video_id:
                    continue
                
                output_path = download_dir / f"tiktok_{keyword.replace(' ', '_')}_{video_id}.mp4"
                
                # Проверяем, существует ли уже этот файл
                if output_path.exists():
                    log.info(f"[TikTok] Video {video_id} already exists for '{keyword}', skipping download")
                    downloaded_videos.append({
                        'id': video_id,
                        'filepath': str(output_path),
                        'keyword': keyword,
                        'source': 'tiktok',
                        'original_url': video_url
                    })
                    continue
                
                log.info(f"[TikTok] Downloading: {video_url}")
                try:
                    # Download video using requests
                    response = requests.get(video_url, stream=True)
                    if response.status_code == 200:
                        with open(output_path, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=1024):
                                if chunk:
                                    f.write(chunk)
                        
                        downloaded_videos.append({
                            'id': video_id,
                            'title': video_info.get('title', f"TikTok_{video_id}"),
                            'original_url': video_info.get('share', video_url),
                            'filepath': output_path,
                            'keyword': keyword
                        })
                        log.info(f"[TikTok] Successfully downloaded: {output_path}")
                    else:
                        log.error(f"[TikTok] Failed to download {video_url}. Status code: {response.status_code}")
                except Exception as e:
                    log.error(f"[TikTok] Failed to download {video_url}. Reason: {e}")
        
        except Exception as e:
            log.error(f"[TikTok] An error occurred during the process for '{keyword}': {e}")
        
        return downloaded_videos
