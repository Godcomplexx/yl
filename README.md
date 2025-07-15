# Cinematic Dataset Collector

Автоматизированный инструмент для создания датасета из кинематографичных видеоклипов с YouTube и TikTok. Система выполняет поиск, загрузку, обработку и валидацию видео до достижения заданного количества уникальных клипов.

## Ключевые возможности

- **Мультиплатформенный сбор данных**: Поддержка YouTube и TikTok с расширяемой архитектурой
- **Интеллектуальная фильтрация**: Отсеивание видео по длительности и наличию водяных знаков
- **Автоматическая обработка**: Нарезка клипов заданной длительности, дедупликация по перцептивному хешу
- **Структурированный вывод**: Организация по категориям, индексация и статистика

## Требования и установка

### Зависимости
- Python 3.8+
- [FFmpeg](https://ffmpeg.org/download.html) в системном PATH
- Пакеты Python: ffmpeg-python, opencv-python, pandas, tqdm, pyyaml, yt_dlp

### Быстрый старт
```bash
# Установка зависимостей
pip install -r requirements.txt

# Запуск системы
python src/main.py
```

## Конфигурация

Настройка через `config.yaml`:

```yaml
# Основные параметры
target_clip_count: 1000  # Целевое количество клипов

# Пути для файлов
paths:
  raw_videos_dir: temp/raw_videos
  dataset_dir: dataset
  logs_dir: logs
  hashes_file: temp/hashes.txt
  index_file: dataset/index.csv
  report_file: dataset/report.md

# Ключевые слова для поиска
keywords:
  - 'cinematic scene'
  - 'smoke effects'
  - 'neon city'

# Активные скраперы
active_scrapers:
  - 'youtube'

# Параметры обработки
processing:
  clip_duration: 5  # секунды
  min_clip_duration: 3  # минимальная длительность видео
  detect_watermarks: true  # включить обнаружение водяных знаков
  watermark_threshold: 30  # порог обнаружения (процент)
```

---

## Структура проекта

```
.
├── config.yaml            # Конфигурация проекта
├── requirements.txt       # Зависимости Python
├── README.md             # Этот файл
├── PROJECT_OVERVIEW.md    # Подробное техническое описание
├── src/                  # Исходный код
│   ├── __init__.py
│   ├── main.py          # Главный скрипт
│   ├── processing.py    # Обработка видео
│   ├── scrapers/        # Модули скраперов
│   │   ├── __init__.py
│   │   ├── base_scraper.py   # Абстрактный класс скрапера
│   │   ├── youtube_scraper.py # Скрапер для YouTube
│   │   └── tiktok_scraper.py  # Скрапер для TikTok с RapidAPI
│   └── utils/          # Утилиты
│       └── logger.py     # Настройка логирования
├── logs/                # Журналы работы (создается автоматически)
└── dataset/             # Собранный датасет (создается автоматически)
```

## Дополнительная информация

Для получения подробной технической информации о каждом компоненте проекта, см. файл [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md).
