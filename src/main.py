import yaml
from pathlib import Path
import shutil
import pandas as pd
from tqdm import tqdm

from utils.logger import setup_logger
from scrapers.youtube_scraper import YouTubeScraper

# A mapping from config string to scraper class
AVAILABLE_SCRAPERS = {
    'youtube': YouTubeScraper,
    # 'tiktok': TikTokScraper, # Example for the future
}
from processing import (
    load_hashes, save_hash, trim_video, 
    calculate_video_hash, is_duplicate, detect_watermark
)


def main():
    """Main function to run the dataset collection pipeline."""
    # 1. Load Configuration and Setup Logger
    config_path = Path('config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    log = setup_logger(Path(config['paths']['logs_dir']))
    log.info("Starting Cinematic Dataset Collection pipeline.")

    # 2. Prepare directories and load existing data
    dataset_dir = Path(config['paths']['dataset_dir'])
    raw_videos_dir = Path(config['paths']['raw_videos_dir'])
    hashes_file = Path(config['paths']['hashes_file'])

    dataset_dir.mkdir(exist_ok=True)
    raw_videos_dir.mkdir(exist_ok=True)

    load_hashes(hashes_file)

    # 3. Scrape Videos
    log.info("--- Starting Video Scraping Phase ---")
    all_downloaded_meta = []
    active_scrapers = config.get('active_scrapers', ['youtube']) # Default to youtube if not specified
    log.info(f"Active scrapers: {active_scrapers}")

    keywords = config['keywords']
    for scraper_name in active_scrapers:
        if scraper_name not in AVAILABLE_SCRAPERS:
            log.warning(f"Scraper '{scraper_name}' is not available. Skipping.")
            continue

        log.info(f"--- Running scraper: {scraper_name} ---")
        scraper_class = AVAILABLE_SCRAPERS[scraper_name]
        scraper_instance = scraper_class()

        for keyword in keywords:
            try:
                video_meta = scraper_instance.search_and_download(keyword, config, raw_videos_dir)
                all_downloaded_meta.extend(video_meta)
            except Exception as e:
                log.critical(f"A critical error occurred in scraper '{scraper_name}' with keyword '{keyword}': {e}")

    log.info(f"Scraping phase complete. Downloaded a total of {len(all_downloaded_meta)} raw videos.")
    if not all_downloaded_meta:
        log.warning("No videos were downloaded. Exiting.")
        return

    # 4. Process Videos and Build Dataset
    log.info("Phase 2: Processing videos - trimming, hashing, and deduplicating.")
    final_clips = []
    clip_id_counter = 0

    for video in tqdm(raw_video_meta, desc="Processing videos"):
        raw_filepath = video['filepath']
        keyword_tag = video['keyword'].replace(' ', '_') # Sanitize tag for folder name

        # Create a directory for the tag
        tag_dir = dataset_dir / keyword_tag
        tag_dir.mkdir(exist_ok=True)

        # Define a path for the trimmed clip
        temp_trimmed_path = raw_videos_dir / f"trimmed_{video['id']}.mp4"

        # Trim the video
        trimmed_path, duration = trim_video(
            input_path=raw_filepath,
            output_path=temp_trimmed_path,
            clip_duration=config['processing']['clip_duration']
        )

        if not trimmed_path or duration < config['processing']['min_clip_duration']:
            log.warning(f"Skipping video {raw_filepath} due to trimming failure or short duration.")
            continue

        # Detect watermark on the trimmed clip
        if detect_watermark(trimmed_path):
            log.info(f"Skipping {trimmed_path} due to potential watermark.")
            trimmed_path.unlink()
            continue

        # Calculate hash and check for duplicates
        video_hash = calculate_video_hash(trimmed_path)
        if not video_hash or is_duplicate(video_hash):
            log.info(f"Skipping {trimmed_path} as it is a duplicate or hashing failed.")
            trimmed_path.unlink() # Clean up temp trimmed file
            continue

        # Save the clip and its hash
        clip_id = f"clip_{clip_id_counter:04d}"
        final_clip_path = tag_dir / f"{clip_id}.mp4"
        shutil.move(str(trimmed_path), str(final_clip_path))
        save_hash(video_hash, hashes_file)

        final_clips.append({
            'clip_id': clip_id,
            'tag': keyword_tag,
            'duration': round(duration, 2),
            'source_url': video['original_url'],
            'perceptual_hash': video_hash
        })
        clip_id_counter += 1
        log.info(f"Added new clip: {final_clip_path}")

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
