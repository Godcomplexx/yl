import logging
from pathlib import Path
from typing import List, Dict

import yt_dlp

from .base_scraper import ScraperStrategy

log = logging.getLogger('CinematicDatasetCollector')

class YouTubeScraper(ScraperStrategy):
    """Scraper strategy for downloading videos from YouTube."""

    def search_and_download(self, keyword: str, config: dict, download_dir: Path) -> List[Dict]:
        """
        Searches YouTube for videos, filters them by duration, and downloads the suitable ones.
        """
        scraper_config = config['scraper']
        search_prefix = scraper_config['search_prefix']
        limit_per_keyword = scraper_config['download_limit_per_keyword']
        max_duration = scraper_config['max_video_duration']

        downloaded_videos = []
        log.info(f"[YouTube] Phase 1: Fetching metadata for keyword: '{keyword}'")
        try:
            # 1. Fetch metadata ONLY, no download
            search_ydl_opts = {
                'quiet': True,
                'ignoreerrors': True,
                'extract_flat': False,  # Get full metadata
                'socket_timeout': 30,   # Add timeout to metadata fetching
            }
            search_query = f"{search_prefix}:{keyword}"

            with yt_dlp.YoutubeDL(search_ydl_opts) as ydl:
                result = ydl.extract_info(search_query, download=False)
                if not result or 'entries' not in result:
                    log.warning(f"[YouTube] No videos found for keyword: {keyword}")
                    return []

            # 2. Filter videos by duration
            suitable_videos = []
            for video in result['entries']:
                if not video: continue
                duration = video.get('duration', 0)
                if 0 < duration <= max_duration:
                    suitable_videos.append(video)
            
            videos_to_download = suitable_videos[:limit_per_keyword]
            log.info(f"[YouTube] Found {len(videos_to_download)} suitable videos (under {max_duration}s) for '{keyword}'.")

            # 3. Download the filtered videos
            if not videos_to_download:
                return []

            log.info(f"[YouTube] Phase 2: Downloading {len(videos_to_download)} videos for keyword: '{keyword}'")
            for i, video in enumerate(videos_to_download):
                try:
                    video_id = video['id']
                    video_url = video['webpage_url']
                    output_path = download_dir / f"youtube_{keyword.replace(' ', '_')}_{video_id}.mp4"
                    
                    # Проверяем, существует ли уже этот файл
                    if output_path.exists():
                        log.info(f"[YouTube] Video {video_id} already exists for '{keyword}', skipping download")
                        downloaded_videos.append({
                            'id': video_id,
                            'filepath': str(output_path),
                            'keyword': keyword,
                            'source': 'youtube',
                            'original_url': video_url
                        })
                        continue
                    
                    download_ydl_opts = {
                        'format': 'bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                        'outtmpl': str(output_path),
                        'quiet': True,
                        'no_warnings': True,
                    }
                    
                    with yt_dlp.YoutubeDL(download_ydl_opts) as ydl:
                        ydl.download([video_url])
                    
                    if output_path.exists():
                        downloaded_videos.append({
                            'id': video_id,
                            'filepath': str(output_path),
                            'keyword': keyword,
                            'source': 'youtube',
                            'original_url': video_url
                        })
                        log.info(f"[YouTube] Downloaded video {i+1}/{len(videos_to_download)} for '{keyword}'")
                    else:
                        log.warning(f"[YouTube] Failed to download video {video_id} for '{keyword}'")
                        
                except Exception as e:
                    log.error(f"[YouTube] Error downloading video: {e}")

        except Exception as e:
            log.error(f"[YouTube] An error occurred during the process for '{keyword}': {e}")

        return downloaded_videos
