import os
import threading
import time
import logging
import json
import gc
import weakref
from urllib.parse import urlparse, urljoin
from collections import defaultdict, deque
from urllib.robotparser import RobotFileParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, PriorityQueue
import multiprocessing
import random
import sqlite3
import pickle
from contextlib import contextmanager
from datetime import datetime, timedelta

from django.conf import settings
from django.utils.text import slugify

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import pandas as pd

# Setup logger
logger = logging.getLogger('optimized_scraper')
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

class OptimizedWebScraper:
    def __init__(self, max_workers=None, cache_enabled=True):
        self.max_workers = max_workers or min(8, multiprocessing.cpu_count() * 2)
        self.cache_enabled = cache_enabled
        self.driver_pool = []
        self.driver_pool_lock = threading.Lock()
        self.url_cache = {}
        self.cache_db_path = os.path.join(settings.BASE_DIR, 'scraper_cache.db')
        self.init_cache()
        
    def init_cache(self):
        """Initialize SQLite cache for URL processing"""
        if self.cache_enabled:
            conn = sqlite3.connect(self.cache_db_path)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS url_cache (
                    url TEXT PRIMARY KEY,
                    content_hash TEXT,
                    last_processed TIMESTAMP,
                    data BLOB
                )
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_url ON url_cache(url)
            ''')
            conn.commit()
            conn.close()
    
    @contextmanager
    def get_driver(self):
        """Get driver from pool or create new one"""
        driver = None
        try:
            with self.driver_pool_lock:
                if self.driver_pool:
                    driver = self.driver_pool.pop()
                else:
                    driver = self.create_driver()
            yield driver
        finally:
            if driver:
                with self.driver_pool_lock:
                    if len(self.driver_pool) < self.max_workers:
                        self.driver_pool.append(driver)
                    else:
                        driver.quit()
    
    def create_driver(self):
    
    def get_driver(self) -> webdriver.Chrome:
        """Get a driver from the pool or create a new one"""
        self.semaphore.acquire()
        
        try:
            driver = self.drivers.get_nowait()
            return driver
        except Empty:
            with self.lock:
                if self.active_drivers < self.pool_size:
                    driver = self.create_driver()
                    self.active_drivers += 1
                    return driver
                else:
                    return self.drivers.get()
    
    def return_driver(self, driver: webdriver.Chrome):
        """Return a driver to the pool"""
        try:
            self.drivers.put_nowait(driver)
        except:
            driver.quit()
        finally:
            self.semaphore.release()
    
    def cleanup(self):
        """Clean up all drivers in the pool"""
        while not self.drivers.empty():
            try:
                driver = self.drivers.get_nowait()
                driver.quit()
            except:
                pass

class BloomFilter:
    """Memory-efficient duplicate detection using bloom filter"""
    
    def __init__(self, size: int = 1000000, hash_count: int = 3):
        self.size = size
        self.hash_count = hash_count
        self.bit_array = [False] * size
    
    def _hashes(self, item: str) -> List[int]:
        """Generate multiple hash values for an item"""
        hashes = []
        for i in range(self.hash_count):
            hash_val = int(hashlib.md5(f"{item}{i}".encode()).hexdigest(), 16)
            hashes.append(hash_val % self.size)
        return hashes
    
    def add(self, item: str):
        """Add an item to the bloom filter"""
        for hash_val in self._hashes(item):
            self.bit_array[hash_val] = True
    
    def contains(self, item: str) -> bool:
        """Check if an item might exist in the bloom filter"""
        for hash_val in self._hashes(item):
            if not self.bit_array[hash_val]:
                return False
        return True

class OptimizedWebScraper:
    """High-performance web scraper with optimized crawling"""
    
    def __init__(self, config: CrawlConfig):
        self.config = config
        self.driver_pool = DriverPool(config.connection_pool_size)
        self.visited_urls = BloomFilter()
        self.url_queue = asyncio.PriorityQueue()
        self.results = []
        self.results_lock = asyncio.Lock()
        self.base_netloc = None
        self.robots_cache = {}
        self.session = None
        
    async def fetch_robots_txt(self, base_url: str) -> Optional[object]:
        """Fetch and parse robots.txt with caching"""
        if base_url in self.robots_cache:
            return self.robots_cache[base_url]
        
        try:
            from urllib.robotparser import RobotFileParser
            rp = RobotFileParser()
            robots_url = urljoin(base_url, '/robots.txt')
            rp.set_url(robots_url)
            rp.read()
            self.robots_cache[base_url] = rp
            return rp
        except Exception as e:
            logger.warning(f"Error fetching robots.txt: {e}")
            return None
    
    async def discover_urls(self, start_url: str) -> Set[str]:
        """Discover all URLs using sitemaps and systematic exploration"""
        discovered = set()
        base_netloc = urlparse(start_url).netloc
        
        # Discover from sitemaps
        sitemap_urls = [
            urljoin(start_url, '/sitemap.xml'),
            urljoin(start_url, '/sitemap_index.xml'),
            urljoin(start_url, '/sitemaps.xml'),
            urljoin(start_url, '/sitemap/sitemap.xml')
        ]
        
        async with aiohttp.ClientSession() as session:
            for sitemap_url in sitemap_urls:
                try:
                    async with session.get(sitemap_url, timeout=10) as response:
                        if response.status == 200:
                            content = await response.text()
                            soup = BeautifulSoup(content, 'xml')
                            for loc in soup.find_all('loc'):
                                url = loc.text.strip()
                                if self.is_valid_url(url, base_netloc):
                                    discovered.add(url)
                except Exception as e:
                    logger.debug(f"Error accessing sitemap {sitemap_url}: {e}")
        
        return discovered
    
    def is_valid_url(self, url: str, base_netloc: str) -> bool:
        """Check if URL is valid and belongs to the target domain"""
        try:
            parsed = urlparse(url)
            return (parsed.scheme in ("http", "https") and 
                    parsed.netloc == base_netloc and
                    not parsed.path.endswith(('.pdf', '.jpg', '.png', '.gif', '.css', '.js')))
        except:
            return False
    
    async def extract_content(self, driver: webdriver.Chrome, url: str) -> List[Dict]:
        """Extract content from a webpage with enhanced selectors"""
        try:
            driver.get(url)
            
            # Wait for page load
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            
            # Scroll to load dynamic content
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            await asyncio.sleep(2)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Remove unwanted elements
            for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                element.extract()
            
            content = []
            content_selectors = [
                'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p',
                'div[class*="content"]', 'div[class*="description"]', 'div[class*="text"]',
                'span[class*="content"]', 'span[class*="description"]', 'span[class*="text"]',
                'li[class*="feature"]', 'li[class*="spec"]', 'li[class*="detail"]',
                'td', 'th', '[class*="spec"]', '[class*="feature"]', '[class*="detail"]'
            ]
            
            for selector in content_selectors:
                elements = soup.select(selector)
                for element in elements:
                    text = element.get_text(strip=True)
                    if text and len(text) > 10:
                        tag_name = element.name or 'content'
                        if element.get('class'):
                            tag_name = f"{tag_name}_{'_'.join(element.get('class', []))}"
                        content.append({
                            'URL': url,
                            'Page Name': soup.title.string.strip() if soup.title else 'No Title',
                            'Heading/Tag': tag_name,
                            'Content': text,
                            'Word Count': len(text.split())
                        })
            
            return content
            
        except Exception as e:
            logger.error(f"Error extracting content from {url}: {e}")
            return []
    
    async def process_url(self, task: CrawlTask) -> List[CrawlTask]:
        """Process a single URL and return new URLs to crawl"""
        if self.visited_urls.contains(task.url):
            return []
        
        self.visited_urls.add(task.url)
        
        driver = self.driver_pool.get_driver()
        try:
            content = await asyncio.get_event_loop().run_in_executor(
                None, self.extract_content, driver, task.url
            )
            
            # Store results
            async with self.results_lock:
                self.results.extend(content)
            
            # Extract new URLs
            new_tasks = []
            # This would normally extract URLs from the content
            # For now, return empty list as we're focusing on optimization
            
            return new_tasks
            
        finally:
            self.driver_pool.return_driver(driver)
    
    async def crawl_worker(self):
        """Worker coroutine to process URLs from the queue"""
        while True:
            try:
                priority, task = await asyncio.wait_for(
                    self.url_queue.get(), timeout=1.0
                )
                
                if task.retry_count > self.config.retry_attempts:
                    continue
                
                new_tasks = await self.process_url(task)
                
                for new_task in new_tasks:
                    await self.url_queue.put((new_task.priority.value, new_task))
                    
            except asyncio.TimeoutError:
                break
            except Exception as e:
                logger.error(f"Error in crawl worker: {e}")
    
    async def crawl(self, start_url: str) -> List[Dict]:
        """Main crawling method"""
        self.base_netloc = urlparse(start_url).netloc
        
        # Discover initial URLs
        discovered_urls = await self.discover_urls(start_url)
        
        # Add discovered URLs to queue
        for url in discovered_urls:
            task = CrawlTask(url=url, depth=0, priority=Priority.MEDIUM)
            await self.url_queue.put((task.priority.value, task))
        
        # Always add start URL
        start_task = CrawlTask(url=start_url, depth=0, priority=Priority.HIGH)
        await self.url_queue.put((start_task.priority.value, start_task))
        
        # Start workers
        workers = [
            asyncio.create_task(self.crawl_worker())
            for _ in range(self.config.max_workers)
        ]
        
        # Wait for completion
        await asyncio.gather(*workers)
        
        return self.results
    
    def save_results(self, filename: str = 'optimized_scraped_data.xlsx'):
        """Save results to Excel file"""
        if not self.results:
            logger.warning("No results to save")
            return
        
        # Group by URL
        grouped = defaultdict(list)
        for item in self.results:
            grouped[item['URL']].append(item)
        
        with pd.ExcelWriter(filename) as writer:
            summary = []
            for url, items in grouped.items():
                df = pd.DataFrame(items)
                sheet_name = url.replace('http://', '').replace('https://', '').replace('/', '_')[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                summary.append({
                    'URL': url,
                    'Total Words': df['Word Count'].sum()
                })
            
