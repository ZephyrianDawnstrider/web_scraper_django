"""
Configuration settings for the optimized web scraper
"""

# Performance Settings
MAX_CONCURRENT_REQUESTS = 8
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_DELAY = 1

# Memory Management
MEMORY_THRESHOLD = 75.0  # Percentage
MAX_MEMORY_USAGE = 85.0

# Caching
CACHE_TTL = 1800  # 30 minutes
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

# WebDriver Settings
WEBDRIVER_TIMEOUT = 15
WEBDRIVER_IMPLICIT_WAIT = 3
MAX_PAGE_SIZE = 10 * 1024 * 1024  # 10MB

# Scraping Limits
MAX_URLS_PER_SESSION = 1000
MAX_DEPTH = 10
RATE_LIMIT_DELAY = 0.5

# User Agent
USER_AGENT = "Mozilla/5.0 (compatible; OptimizedScraper/1.0)"

# Chrome Options
CHROME_OPTIONS = [
    '--headless',
    '--no-sandbox',
    '--disable-dev-shm-usage',
    '--disable-gpu',
    '--disable-extensions',
    '--disable-images',
    '--disable-plugins',
    '--disable-background-timer-throttling',
    '--disable-backgrounding-occluded-windows',
    '--disable-renderer-backgrounding',
    '--memory-pressure-off',
    '--max_old_space_size=512'
]

# Logging
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Output Settings
OUTPUT_FORMAT = 'excel'
OUTPUT_FILE = 'scraped_data.xlsx'
MAX_OUTPUT_SIZE = 100 * 1024 * 1024  # 100MB
