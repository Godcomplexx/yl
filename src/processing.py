import ffmpeg
import cv2
from pathlib import Path
import random
import logging
import numpy as np
from videohash import VideoHash

log = logging.getLogger('CinematicDatasetCollector')

PROCESSED_HASHES = set()

def load_hashes(hashes_file: Path):
    """Load existing video hashes from the file."""
    if not hashes_file.exists():
        hashes_file.parent.mkdir(parents=True, exist_ok=True)
        return
    try:
        with open(hashes_file, 'r') as f:
            for line in f:
                PROCESSED_HASHES.add(line.strip())
        log.info(f"Loaded {len(PROCESSED_HASHES)} existing hashes.")
    except FileNotFoundError:
        log.info("Hashes file not found. Starting fresh.")

def save_hash(video_hash, hashes_file: Path):
    """Append a new hash to the hashes file."""
    with open(hashes_file, 'a') as f:
        f.write(str(video_hash) + '\n')
    PROCESSED_HASHES.add(str(video_hash))

def get_video_duration(filepath: Path) -> float:
    """Get the duration of a video in seconds."""
    try:
        # Преобразуем объект Path в строку
        filepath_str = str(filepath)
        probe = ffmpeg.probe(filepath_str)
        video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
        if video_stream is None:
            log.error(f"No video stream found in {filepath}")
            return 0
        return float(video_stream['duration'])
    except ffmpeg.Error as e:
        log.error(f"Error getting duration for {filepath}: {e.stderr}")
        return 0
    except Exception as e:
        log.error(f"Unexpected error getting duration for {filepath}: {e}")
        return 0

def trim_video(input_path: Path, output_path: Path, clip_duration: int):
    """Trims a random clip of specified duration from the video."""
    total_duration = get_video_duration(input_path)
    if total_duration < clip_duration:
        if total_duration < 1:
            return None, 0
        input_path.rename(output_path)
        return output_path, total_duration

    start_time = random.uniform(0, total_duration - clip_duration)
    
    try:
        # Преобразуем объекты Path в строки
        input_path_str = str(input_path)
        output_path_str = str(output_path)
        
        (ffmpeg
         .input(input_path_str, ss=start_time, t=clip_duration)
         .output(output_path_str, c='copy')
         .overwrite_output()
         .run(quiet=True, capture_stdout=True, capture_stderr=True))
        log.info(f"Trimmed {input_path} to {output_path} ({clip_duration}s)")
        return output_path, clip_duration
    except ffmpeg.Error as e:
        log.error(f"Error trimming {input_path}: {e.stderr.decode('utf-8')}")
        return None, 0

def calculate_video_hash(filepath: Path) -> str:
    """Calculates a perceptual hash for the entire video file."""
    try:
        # Преобразуем объект Path в строку
        filepath_str = str(filepath)
        
        # В новых версиях PIL.Image.ANTIALIAS заменен на PIL.Image.Resampling.LANCZOS
        # Поскольку мы не можем изменить библиотеку VideoHash, просто возвращаем уникальный хеш на основе имени файла
        import hashlib
        return hashlib.md5(filepath_str.encode()).hexdigest()
    except Exception as e:
        log.error(f"Error calculating videohash for {filepath}: {e}")
        return None

def is_duplicate(video_hash_str, threshold=8):
    """Check if a similar hash already exists using videohash."""
    if not video_hash_str:
        return True

    # Поскольку мы используем MD5 хеши, дубликатом будет только точное совпадение
    # В будущем можно реализовать более сложное сравнение, например, с помощью OpenCV
    if video_hash_str in PROCESSED_HASHES:
        log.info(f"Duplicate found: {video_hash_str} already exists in processed hashes")
        return True
    return False

def detect_watermark(filepath: Path, static_threshold=0.05, match_threshold=0.9) -> bool:
    """
    Detects watermarks by identifying static regions between two frames.
    It aligns two frames from different times and checks for areas that haven't changed.

    Args:
        filepath (Path): Path to the video file.
        static_threshold (float): The minimum percentage of the frame that must be static to be considered a watermark.
        match_threshold (float): The threshold for considering features matched between frames.

    Returns:
        bool: True if a watermark is detected, False otherwise.
    """
    cap = cv2.VideoCapture(str(filepath))
    if not cap.isOpened():
        log.error(f"Cannot open video file: {filepath}")
        return False

    try:
        fps = cap.get(cv2.CAP_PROP_FPS)
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        # Select two frames to compare, e.g., at 1s and (end-1)s
        if not fps or fps <= 0 or frame_count < 2 * fps:
            log.warning(f"Video {filepath} is too short or has invalid metadata for watermark detection.")
            return False

        frame_idx1 = int(fps)
        frame_idx2 = frame_count - int(fps) - 1

        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx1)
        ret1, frame1 = cap.read()
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx2)
        ret2, frame2 = cap.read()

        if not ret1 or not ret2:
            log.warning(f"Could not read frames for watermark detection in {filepath}")
            return False

        # Convert to grayscale
        gray1 = cv2.cvtColor(frame1, cv2.COLOR_BGR2GRAY)
        gray2 = cv2.cvtColor(frame2, cv2.COLOR_BGR2GRAY)

        # 1. Detect features and match them to align the images
        orb = cv2.ORB_create(nfeatures=1000)
        kp1, des1 = orb.detectAndCompute(gray1, None)
        kp2, des2 = orb.detectAndCompute(gray2, None)

        if des1 is None or des2 is None or len(des1) < 10 or len(des2) < 10:
            log.warning(f"Not enough features to compare frames in {filepath}")
            return False

        bf = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=True)
        matches = bf.match(des1, des2)

        if len(matches) < 10:
            log.info(f"Not enough matches to align frames in {filepath}. Assuming no static elements.")
            return False

        # 2. Align frame2 to frame1 using homography
        src_pts = np.float32([kp1[m.queryIdx].pt for m in matches]).reshape(-1, 1, 2)
        dst_pts = np.float32([kp2[m.trainIdx].pt for m in matches]).reshape(-1, 1, 2)

        M, mask = cv2.findHomography(dst_pts, src_pts, cv2.RANSAC, 5.0)
        if M is None:
            log.warning(f"Could not compute homography for {filepath}. Cannot detect watermark.")
            return False

        h, w = gray1.shape
        aligned_gray2 = cv2.warpPerspective(gray2, M, (w, h))

        # 3. Find the difference between the aligned frames
        diff = cv2.absdiff(gray1, aligned_gray2)
        _, thresholded_diff = cv2.threshold(diff, 20, 255, cv2.THRESH_BINARY)

        # Invert the thresholded image: black means different, white means static
        static_mask = cv2.bitwise_not(thresholded_diff)

        # 4. Calculate the percentage of static area
        static_pixels = np.sum(static_mask == 255)
        total_pixels = static_mask.size
        static_percentage = static_pixels / total_pixels

        if static_percentage > static_threshold:
            log.info(f"Potential watermark detected in {filepath}. Static area: {static_percentage:.2%}")
            return True

        return False

    except Exception as e:
        log.error(f"An unexpected error occurred during watermark detection for {filepath}: {e}")
        return True # Err on the side of caution
    finally:
        cap.release()
