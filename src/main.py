import yaml
from pathlib import Path
import shutil
import pandas as pd
from tqdm import tqdm

from utils.logger import setup_logger
from scrapers.youtube_scraper import YouTubeScraper
from scrapers.tiktok_scraper import TikTokScraper

# A mapping from config string to scraper class
AVAILABLE_SCRAPERS = {
    'youtube': YouTubeScraper,
    'tiktok': TikTokScraper,
}
from processing import (
    load_hashes, save_hash, trim_video, 
    calculate_video_hash, is_duplicate, detect_watermark,
    get_video_duration
)


def process_video(video_meta, config, dataset_dir, hashes_file, clip_id_counter, log):
    """
    Обрабатывает одно видео: проверяет длительность, водяные знаки, создает клип, проверяет на дубликаты.
    
    Args:
        video_meta (dict): Метаданные видео (путь, ключевое слово и т.д.)
        config (dict): Конфигурация проекта
        dataset_dir (Path): Путь к директории датасета
        hashes_file (Path): Путь к файлу с хешами
        clip_id_counter (int): Текущий счетчик клипов
        log (Logger): Логгер
        
    Returns:
        tuple: (clip_meta, new_clip_id_counter) - метаданные клипа и обновленный счетчик, или (None, clip_id_counter) если клип не создан
    """
    raw_filepath = Path(video_meta['filepath'])
    keyword_tag = video_meta['keyword'].replace(' ', '_')  # Sanitize tag for folder name
    
    try:
        # 1. Проверяем длительность видео
        duration = get_video_duration(raw_filepath)
        if duration < config['processing']['min_clip_duration']:
            log.info(f"Skipping video (too short): {raw_filepath}")
            return None, clip_id_counter

        # 2. Проверяем наличие водяных знаков
        if config['processing'].get('detect_watermarks', False):
            # Используем порог из конфигурации или по умолчанию 30%
            watermark_threshold = config['processing'].get('watermark_threshold', 30.0) / 100.0
            if detect_watermark(raw_filepath, static_threshold=watermark_threshold):
                log.info(f"Skipping video (watermark detected): {raw_filepath}")
                return None, clip_id_counter

        # 3. Создаем клип
        clip_duration = min(duration, config['processing']['max_clip_duration'])
        clip_id = f"clip_{clip_id_counter:04d}"
        clip_id_counter += 1
        
        # Создаем директорию для категории
        category_dir = dataset_dir / keyword_tag
        category_dir.mkdir(exist_ok=True)
        
        # Создаем путь для клипа
        clip_path = category_dir / f"{clip_id}.mp4"
        
        # Обрезаем видео
        trim_video(raw_filepath, clip_path, clip_duration)
        
        # 4. Проверяем на дубликаты
        video_hash = calculate_video_hash(clip_path)
        if is_duplicate(str(video_hash)):
            log.info(f"Skipping clip (duplicate detected): {clip_path}")
            clip_path.unlink(missing_ok=True)  # Удаляем дубликат
            return None, clip_id_counter
            
        # Сохраняем хеш
        save_hash(video_hash, hashes_file)
        
        # Создаем метаданные клипа
        clip_meta = {
            'id': clip_id,
            'path': str(clip_path),
            'tag': keyword_tag,
            'duration': clip_duration,
            'source': video_meta.get('source', 'unknown'),
            'keyword': video_meta['keyword']
        }
        
        log.info(f"Successfully created clip: {clip_path}")
        return clip_meta, clip_id_counter
        
    except Exception as e:
        log.error(f"Error processing video {raw_filepath}: {e}")
        return None, clip_id_counter


def main():
    """Main function to run the dataset collection pipeline."""
    # 1. Load Configuration
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    log = setup_logger(Path(config['paths']['logs_dir']))
    log.info("Starting Cinematic Dataset Collection pipeline.")

    # 2. Prepare directories
    raw_videos_dir = Path(config['paths']['raw_videos_dir'])
    dataset_dir = Path(config['paths']['dataset_dir'])
    logs_dir = Path(config['paths']['logs_dir'])
    hashes_file = Path(config['paths']['hashes_file'])

    # Create directories with parents=True to ensure parent directories are created
    raw_videos_dir.mkdir(exist_ok=True, parents=True)
    dataset_dir.mkdir(exist_ok=True, parents=True)
    logs_dir.mkdir(exist_ok=True, parents=True)

    # Загружаем существующие хеши для дедупликации
    load_hashes(hashes_file)
    
    # Устанавливаем целевое количество клипов в датасете
    target_clip_count = config.get('target_clip_count', 1000)  # По умолчанию 1000 клипов
    log.info(f"Target clip count: {target_clip_count} clips in dataset")
    
    # Загружаем счетчик клипов
    clip_counter_file = Path('temp/clip_counter.txt')
    if clip_counter_file.exists():
        with open(clip_counter_file, 'r') as f:
            clip_id_counter = int(f.read().strip())
        log.info(f"Loaded clip counter: {clip_id_counter}")
    else:
        clip_id_counter = 0
    
    # Подсчитываем существующие клипы в датасете
    existing_clips_count = 0
    if dataset_dir.exists():
        # Рекурсивный поиск всех MP4 файлов в датасете
        existing_clips_count = len(list(dataset_dir.glob('**/*.mp4')))
        log.info(f"Found {existing_clips_count} existing clips in dataset")
    
    # Проверяем, достигнуто ли целевое количество клипов
    if existing_clips_count >= target_clip_count:
        log.info(f"Target clip count of {target_clip_count} already reached. Current clips: {existing_clips_count}. Exiting.")
        return
    
    # Список для хранения метаданных о финальных клипах
    final_clips = []

    # 3. Scrape Videos
    log.info("--- Starting Video Scraping Phase ---")
    all_downloaded_meta = []
    active_scrapers = config.get('active_scrapers', ['youtube']) # Default to youtube if not specified
    log.info(f"Active scrapers: {active_scrapers}")
    
    # Создаем файлы для отслеживания прогресса
    progress_file = Path('temp/scraping_progress.txt')
    processing_progress_file = Path('temp/processing_progress.txt')
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    processing_progress_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Загружаем информацию о уже обработанных ключевых словах и скраперах
    processed_items = set()
    if progress_file.exists():
        with open(progress_file, 'r') as f:
            for line in f:
                processed_items.add(line.strip())
        log.info(f"Found progress file with {len(processed_items)} processed items.")
    
    # Загружаем информацию о уже обработанных видео
    processed_videos = set()
    if processing_progress_file.exists():
        with open(processing_progress_file, 'r') as f:
            for line in f:
                processed_videos.add(line.strip())
        log.info(f"Found processing progress file with {len(processed_videos)} processed videos.")
        
    # Собираем информацию о существующих видеофайлах
    existing_videos = []
    if raw_videos_dir.exists():
        log.info("Scanning for existing video files...")
        for video_file in raw_videos_dir.glob("*.mp4"):
            # Извлекаем метаданные из имени файла (формат: source_keyword_id.mp4)
            try:
                filename = video_file.stem
                parts = filename.split('_')
                if len(parts) >= 3:
                    source = parts[0]
                    video_id = parts[-1]
                    keyword = '_'.join(parts[1:-1]).replace('_', ' ')
                    
                    video_meta = {
                        'id': video_id,
                        'filepath': str(video_file),
                        'keyword': keyword,
                        'source': source
                    }
                    existing_videos.append(video_meta)
                    log.info(f"Found existing video: {video_file.name} (keyword: {keyword})")
            except Exception as e:
                log.warning(f"Could not parse metadata from filename {video_file.name}: {e}")
                
        log.info(f"Found {len(existing_videos)} existing video files.")
        all_downloaded_meta.extend(existing_videos)
        
    # Обрабатываем существующие видео перед скачиванием новых
    if existing_videos:
        log.info(f"Processing {len(existing_videos)} existing videos before downloading new ones...")
        for video_meta in tqdm(existing_videos, desc="Processing existing videos"):
            # Проверяем, не обрабатывали ли мы уже это видео
            video_id = f"{video_meta['source']}_{video_meta['id']}"
            if video_id in processed_videos:
                log.info(f"Video {video_id} already processed, skipping")
                continue
            
            # Обрабатываем видео
            clip_meta, clip_id_counter = process_video(video_meta, config, dataset_dir, hashes_file, clip_id_counter, log)
            
            # Сохраняем счетчик клипов
            with open(clip_counter_file, 'w') as f:
                f.write(str(clip_id_counter))
            
            # Если клип успешно создан, добавляем его в список
            if clip_meta:
                final_clips.append(clip_meta)
                existing_clips_count += 1
                log.info(f"Clips in dataset: {existing_clips_count}/{target_clip_count}")
            
            # Отмечаем видео как обработанное
            with open(processing_progress_file, 'a') as f:
                f.write(f"{video_id}\n")
            processed_videos.add(video_id)
            
            # Проверяем достижение целевого количества клипов
            if existing_clips_count >= target_clip_count:
                log.info(f"Reached target clip count of {target_clip_count} with existing videos. Stopping processing.")
                # Сохраняем счетчик клипов для следующего запуска
                with open(clip_counter_file, 'w') as f:
                    f.write(str(clip_id_counter))
                log.info(f"Scraping and processing phase complete.")
                log.info(f"Created a total of {len(final_clips)} clips.")
                log.info(f"Final clip count: {existing_clips_count}/{target_clip_count}")
                return
    
    # Устанавливаем целевое количество клипов в датасете
    target_clip_count = config.get('target_clip_count', 1000)  # По умолчанию 1000 клипов
    
    # Коэффициент запаса - сколько видео нужно скачать для получения целевого количества клипов
    margin_factor = config.get('video_margin_factor', 1.5)
    
    # Рассчитываем целевое количество видео для скачивания
    target_video_count = int(target_clip_count * margin_factor)
    log.info(f"Target: {target_clip_count} clips in dataset, planning to download up to {target_video_count} videos")
    
    # Загружаем счетчик скачанных видео
    video_count_file = Path('temp/video_count.txt')
    total_videos_downloaded = len(existing_videos)
    if video_count_file.exists():
        try:
            with open(video_count_file, 'r') as f:
                total_videos_downloaded = int(f.read().strip())
            log.info(f"Loaded video count: {total_videos_downloaded} videos already downloaded")
        except Exception as e:
            log.warning(f"Could not load video count: {e}")
    
    # Загружаем информацию о существующих клипах в датасете
    existing_clips_count = 0
    if dataset_dir.exists():
        # Подсчитываем количество существующих клипов в датасете
        existing_clips_count = len(list(dataset_dir.glob('**/*.mp4')))
        log.info(f"Found {existing_clips_count} existing clips in dataset")
    
    # Сохраняем текущий счетчик видео
    with open(video_count_file, 'w') as f:
        f.write(str(total_videos_downloaded))
    
    keywords = config['keywords']
    for scraper_name in active_scrapers:
        if scraper_name not in AVAILABLE_SCRAPERS:
            log.warning(f"Scraper '{scraper_name}' is not available. Skipping.")
            continue

        log.info(f"--- Running scraper: {scraper_name} ---")
        scraper_class = AVAILABLE_SCRAPERS[scraper_name]
        scraper_instance = scraper_class()

        for keyword in keywords:
            # Проверяем, достигли ли мы целевого количества видео
            if total_videos_downloaded >= target_video_count:
                log.info(f"Reached target video count of {target_video_count}. Stopping scraping.")
                break
            
            # Проверяем, была ли эта комбинация скрапер+ключевое слово уже обработана
            progress_key = f"{scraper_name}:{keyword}"
            if progress_key in processed_items:
                log.info(f"Skipping already processed combination: {progress_key}")
                continue
                
            try:
                log.info(f"Processing {scraper_name} with keyword '{keyword}'")
                video_meta_list = scraper_instance.search_and_download(keyword, config, raw_videos_dir)
                all_downloaded_meta.extend(video_meta_list)
                total_videos_downloaded += len(video_meta_list)
                
                # Сохраняем прогресс после успешной обработки
                with open(progress_file, 'a') as f:
                    f.write(f"{progress_key}\n")
                
                # Сохраняем общее количество скачанных видео
                with open(video_count_file, 'w') as f:
                    f.write(str(total_videos_downloaded))
                
                log.info(f"Total videos downloaded so far: {total_videos_downloaded}/{target_video_count}")
                
                # Обрабатываем каждое скачанное видео сразу после скачивания
                log.info(f"Processing {len(video_meta_list)} videos for keyword '{keyword}'")
                for video_meta in video_meta_list:
                    # Проверяем, не обрабатывали ли мы уже это видео
                    video_id = f"{video_meta['source']}_{video_meta['id']}"
                    if video_id in processed_videos:
                        log.info(f"Video {video_id} already processed, skipping")
                        continue
                    
                    # Обрабатываем видео
                    clip_meta, clip_id_counter = process_video(video_meta, config, dataset_dir, hashes_file, clip_id_counter, log)
                    
                    # Сохраняем счетчик клипов
                    with open(clip_counter_file, 'w') as f:
                        f.write(str(clip_id_counter))
                    
                    # Если клип успешно создан, добавляем его в список
                    if clip_meta:
                        final_clips.append(clip_meta)
                        existing_clips_count += 1
                        log.info(f"Clips in dataset: {existing_clips_count}/{target_clip_count}")
                    
                    # Отмечаем видео как обработанное
                    with open(processing_progress_file, 'a') as f:
                        f.write(f"{video_id}\n")
                    processed_videos.add(video_id)
                    
                    # Проверяем достижение целевого количества клипов
                    if existing_clips_count >= target_clip_count:
                        log.info(f"Reached target clip count of {target_clip_count}. Stopping processing.")
                        break
                
                # Проверяем достижение целевого количества клипов после обработки всех видео по ключевому слову
                if existing_clips_count >= target_clip_count:
                    log.info(f"Reached target clip count of {target_clip_count}. Stopping scraping.")
                    break
                    
            except Exception as e:
                log.critical(f"A critical error occurred in scraper '{scraper_name}' with keyword '{keyword}': {e}")
                
        # Проверяем достижение целевого количества клипов после каждого скрапера
        if existing_clips_count >= target_clip_count:
            log.info(f"Reached target clip count of {target_clip_count}. Stopping scraping.")
            break

    # Сохраняем счетчик клипов для следующего запуска
    with open(clip_counter_file, 'w') as f:
        f.write(str(clip_id_counter))
        
    log.info(f"Scraping and processing phase complete.")
    log.info(f"Created a total of {len(final_clips)} clips.")
    log.info(f"Final clip count: {existing_clips_count}/{target_clip_count}")
    log.info(f"Downloaded a total of {len(all_downloaded_meta)} raw videos.")

    # 5. Create Index File and Clean Up
    log.info("Phase 3: Finalizing dataset.")
    if final_clips:
        index_df = pd.DataFrame(final_clips)
        index_path = Path(config['paths']['index_file'])
        index_df.to_csv(index_path, index=False)
        log.info(f"Dataset index created at {index_path}")
    else:
        log.warning("No clips were added to the dataset.")

    # Clean up raw video directory
    try:
        shutil.rmtree(raw_videos_dir)
        log.info(f"Cleaned up temporary directory: {raw_videos_dir}")
    except OSError as e:
        log.error(f"Error removing temporary directory {raw_videos_dir}: {e}")

    log.info(f"Pipeline finished. Collected {len(final_clips)} unique clips.")

    # 6. Generate Report and Archive
    if final_clips:
        log.info("Phase 4: Generating report and creating archive.")
        report_path = Path(config['paths']['report_file'])
        generate_report(final_clips, report_path)
        log.info(f"Report generated at {report_path}")

        archive_name = 'cinematic_dataset'
        shutil.make_archive(archive_name, 'zip', dataset_dir)
        log.info(f"Dataset archived to {archive_name}.zip")


def generate_report(clips_metadata, report_path):
    """Generates a markdown report from the collected clips metadata."""
    df = pd.DataFrame(clips_metadata)
    
    total_clips = len(df)
    avg_duration = df['duration'].mean()
    tag_counts = df['tag'].value_counts().to_dict()

    with open(report_path, 'w') as f:
        f.write("# Cinematic Dataset Collection Report\n\n")
        f.write("## Summary\n\n")
        f.write(f"- **Total Clips Collected:** {total_clips}\n")
        f.write(f"- **Average Clip Duration:** {avg_duration:.2f} seconds\n\n")
        f.write("## Clips per Tag\n\n")
        f.write("| Tag | Number of Clips |\n")
        f.write("|-----|-----------------|\n")
        for tag, count in tag_counts.items():
            f.write(f"| `{tag}` | {count} |\n")


if __name__ == '__main__':
    main()
