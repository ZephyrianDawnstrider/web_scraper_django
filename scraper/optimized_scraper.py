import asyncio
import aiohttp
import async_timeout
import logging
import json
import time
import hashlib
from urllib.parse import urlparse, urljoin
from collections import defaultdict
from typing import Dict, List, Set, Optional
import redis
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import psutil
import gc

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('optimized_scraper')

class OptimizedWebScraper:
    def __init__(self, 
                 max_concurrent_requests: int = 10,
                 cache_ttl: int = 3600,
                 redis_host: str = 'localhost',
                 redis_port: int = 6379,
                 memory_threshold: float = 80.0):
        
        self.max_concurrent_requests = max_concurrent_requests
        self.cache_ttl = cache_ttl
        self.memory_threshold = memory_threshold
        
        # Initialize Redis for caching
        try:
            self.redis_client = redis.Redis(
                host=redis_host, 
                port=redis_port, 
                decode_responses=True,
                socket_connect_timeout=5
            )
            self.redis_client.ping()
            logger.info("Redis connection established")
        except:
            logger.warning("Redis not available, caching disabled")
            self.redis_client = None
        
        # Thread-safe sets for deduplication
        self.processed_urls: Set[str] = set()
        self.failed_urls: Set[str] = set()
        
        # Statistics
        self.stats = {
            'total_requests': 0,
            'cache_hits': 0,
            'successful_scrapes': 0,
            'failed_scrapes': 0,
            'memory_usage': []
        }
        
    def _get_cache_key(self, url: str) -> str:
        """Generate cache key for URL"""
        return f"scrape:{hashlib.md5(url.encode()).hexdigest()}"
    
    def _check_memory_usage(self) -> float:
        """Check current memory usage percentage"""
        memory = psutil.virtual_memory()
        usage_percent = memory.percent
        self.stats['memory_usage'].append(usage_percent)
        
        if usage_percent > self.memory_threshold:
            logger.warning(f"High memory usage: {usage_percent}%")
            gc.collect()
        
        return usage_percent
    
    async def _get_cached_response(self, url: str) -> Optional[Dict]:
        """Get cached response from Redis"""
        if not self.redis_client:
            return None
            
        cache_key = self._get_cache_key(url)
        cached = self.redis_client.get(cache_key)
        
        if cached:
            self.stats['cache_hits'] += 1
            return json.loads(cached)
        
        return None
    
    async def _cache_response(self, url: str, data: Dict):
        """Cache response in Redis"""
        if not self.redis_client:
            return
            
        cache_key = self._get_cache_key(url)
        self.redis_client.setex(
            cache_key, 
            self.cache_ttl, 
            json.dumps(data, ensure_ascii=False)
        )
    
    def _create_optimized_driver(self) -> webdriver.Chrome:
        """Create optimized Chrome WebDriver instance with enhanced timeout handling"""
        options = Options()
        
        # Critical timeout and performance optimizations
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-images')
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-backgrounding-occluded-windows')
        options.add_argument('--disable-renderer-backgrounding')
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-features=VizDisplayCompositor')
        
        # Memory and resource optimizations
        options.add_argument('--memory-pressure-off')
        options.add_argument('--max_old_space_size=256')
        options.add_argument('--disk-cache-size=1')
        options.add_argument('--media-cache-size=1')
        
        # Network optimizations
        options.add_argument('--disable-application-cache')
        options.add_argument('--disable-browser-side-navigation')
        options.add_argument('--disable-default-apps')
        options.add_argument('--disable-extensions-file-access-check')
        options.add_argument('--disable-extensions-http-throttling')
        
        # User agent rotation
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Use webdriver-manager for automatic driver management
        service = Service(ChromeDriverManager().install())
        
        # Enhanced timeout configuration
        from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
        
        caps = DesiredCapabilities.CHROME.copy()
        caps['pageLoadStrategy'] = 'eager'  # Don't wait for all resources
        caps['goog:loggingPrefs'] = {'performance': 'ALL'}
        
        driver = webdriver.Chrome(
            service=service, 
            options=options,
            desired_capabilities=caps
        )
        
        # Aggressive timeout settings
        driver.set_page_load_timeout(10)  # Reduced from 15
        driver.set_script_timeout(5)
        driver.implicitly_wait(2)  # Reduced from 3
        
        return driver
        
    async def _scrape_single_url(self, url: str, session: aiohttp.ClientSession) -> Dict:
        """Scrape a single URL with optimizations"""
        start_time = time.time()
        
        # Check cache first
        cached = await self._get_cached_response(url)
        if cached:
            logger.info(f"Cache hit for {url}")
            return cached
        
        # Check if already processed
        if url in self.processed_urls:
            return {'url': url, 'status': 'already_processed'}
        
        self.processed_urls.add(url)
        self.stats['total_requests'] += 1
        
        # Check memory usage
        self._check_memory_usage()
        
        driver = None
        try:
            driver = self._create_optimized_driver()
            driver.get(url)
            
            # Wait for page load
            await asyncio.sleep(2)
            
            # Get page content
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract data
            page_name = soup.title.string.strip() if soup.title else "No Title"
            
            # Remove unwanted elements
            for element in soup(['script', 'style', 'nav', 'header', 'footer']):
                element.decompose()
            
            # Extract content
            content = []
            for element in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p']):
                text = element.get_text(strip=True)
                if text and len(text) > 10:
                    content.append({
                        'tag': element.name,
                        'text': text,
                        'word_count': len(text.split())
                    })
            
            result = {
                'url': url,
                'page_name': page_name,
                'content': content,
                'timestamp': time.time(),
                'processing_time': time.time() - start_time
            }
            
            # Cache the result
            await self._cache_response(url, result)
            
            self.stats['successful_scrapes'] += 1
            logger.info(f"Successfully scraped {url} in {result['processing_time']:.2f}s")
            
            return result
            
        except Exception as e:
            self.stats['failed_scrapes'] += 1
            self.failed_urls.add(url)
            logger.error(f"Failed to scrape {url}: {str(e)}")
            return {'url': url, 'error': str(e), 'status': 'failed'}
            
        finally:
            if driver:
                driver.quit()
                del driver
    
    async def scrape_urls(self, urls: List[str]) -> List[Dict]:
        """Scrape multiple URLs concurrently"""
        logger.info(f"Starting optimized scraping of {len(urls)} URLs")
        
        # Create semaphore for rate limiting
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        async def _bounded_scrape(url, session):
            async with semaphore:
                return await self._scrape_single_url(url, session)
        
        # Create aiohttp session with connection pooling
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent_requests * 2,
            limit_per_host=self.max_concurrent_requests
        )
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as session:
            tasks = [_bounded_scrape(url, session) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions
        valid_results = [r for r in results if isinstance(r, dict) and 'error' not in r]
        
        logger.info(f"Scraping completed: {len(valid_results)} successful, {len(self.failed_urls)} failed")
        
        return valid_results
    
    def get_statistics(self) -> Dict:
        """Get scraping statistics"""
        return {
            **self.stats,
            'processed_urls': len(self.processed_urls),
            'failed_urls': len(self.failed_urls),
            'current_memory_usage': self._check_memory_usage()
        }
    
    def cleanup(self):
        """Cleanup resources"""
        if self.redis_client:
            self.redis_client.close()
        
        # Force garbage collection
        gc.collect()
        
        logger.info("Cleanup completed")

# Usage example
async def main():
    scraper = OptimizedWebScraper(
        max_concurrent_requests=8,
        cache_ttl=1800,
        memory_threshold=75.0
    )
    
    urls = [
        'https://example.com',
        'https://example.com/about',
        'https://example.com/contact'
    ]
    
    try:
        results = await scraper.scrape_urls(urls)
        stats = scraper.get_statistics()
        
        print(f"Scraped {len(results)} URLs successfully")
        print(f"Statistics: {stats}")
        
        # Save to Excel
        df = pd.DataFrame([
            {
                'URL': item['url'],
                'Page Name': item['page_name'],
                'Content Count': len(item['content']),
                'Processing Time': item['processing_time']
            }
            for item in results
        ])
        
        df.to_excel('optimized_scraped_data.xlsx', index=False)
        print("Data saved to optimized_scraped_data.xlsx")
        
    finally:
        scraper.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
