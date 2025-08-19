import os
import threading
import time
import logging
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
from threading import Lock
import multiprocessing
from contextlib import contextmanager
import psutil
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Performance logger
perf_logger = logging.getLogger('performance')
if not perf_logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - PERFORMANCE - %(message)s')
    handler.setFormatter(formatter)
    perf_logger.addHandler(handler)
    perf_logger.setLevel(logging.INFO)

class ChromeDriverPool:
    """Thread-safe Chrome driver pool for reuse"""
    
    def __init__(self, max_drivers=4):
        self.max_drivers = max_drivers
        self.drivers = Queue(maxsize=max_drivers)
        self.lock = Lock()
        self.created_drivers = 0
        
    def _create_driver(self):
        """Create optimized Chrome driver"""
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-images")  # Disable images for speed
        options.add_argument("--disable-javascript")  # Disable JS if not needed
        options.add_argument("--blink-settings=imagesEnabled=false")
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        
        # Performance preferences
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.notifications": 2,
            "profile.managed_default_content_settings.stylesheets": 2,
            "profile.managed_default_content_settings.cookies": 2,
            "profile.managed_default_content_settings.javascript": 1,  # Keep JS for dynamic content
            "disk-cache-size": 4096
        }
        options.add_experimental_option("prefs", prefs)
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(20)  # Reduced timeout
        return driver
    
    @contextmanager
    def get_driver(self):
        """Get driver from pool or create new one"""
        driver = None
        try:
            driver = self.drivers.get_nowait()
        except Empty:
            with self.lock:
                if self.created_drivers < self.max_drivers:
                    driver = self._create_driver()
                    self.created_drivers += 1
                    perf_logger.info(f"Created new driver #{self.created_drivers}")
                else:
                    # Wait for available driver
                    driver = self.drivers.get(timeout=10)
        
        try:
            yield driver
        finally:
            if driver:
                try:
                    self.drivers.put_nowait(driver)
                except:
                    driver.quit()
    
    def cleanup(self):
        """Close all drivers"""
        while not self.drivers.empty():
            try:
                driver = self.drivers.get_nowait()
                driver.quit()
            except:
                pass

class MemoryEfficientScraper:
    """Memory-efficient scraper with chunked processing"""
    
    def __init__(self, chunk_size=100):
        self.chunk_size = chunk_size
        self.driver_pool = ChromeDriverPool()
        
    def process_urls_batch(self, urls_batch, process_func):
        """Process URLs in batches to manage memory"""
        results = []
        
        with ThreadPoolExecutor(max_workers=min(4, multiprocessing.cpu_count())) as executor:
            future_to_url = {
                executor.submit(process_func, url): url 
                for url in urls_batch
            }
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result(timeout=30)
                    if result:
                        results.extend(result)
                        
                    # Force garbage collection every 10 URLs
                    if len(results) % 10 == 0:
                        gc.collect()
                        
                except Exception as e:
                    perf_logger.error(f"Error processing {url}: {e}")
        
        return results
    
    def chunked_excel_export(self, data, filename, chunk_size=1000):
        """Export data to Excel in chunks to reduce memory usage"""
        if not data:
            return
            
        # Process in chunks
        total_chunks = (len(data) + chunk_size - 1) // chunk_size
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                df_chunk = pd.DataFrame(chunk)
                
                sheet_name = f'Data_{i//chunk_size + 1}'
                df_chunk.to_excel(
                    writer, 
                    sheet_name=sheet_name, 
                    index=False,
                    startrow=0 if i == 0 else None
                )
                
                # Force garbage collection
                del df_chunk
                gc.collect()
                
                perf_logger.info(f"Exported chunk {i//chunk_size + 1}/{total_chunks}")
    
    def get_memory_usage(self):
        """Get current memory usage"""
        process = psutil.Process()
        return {
            'memory_mb': process.memory_info().rss / 1024 / 1024,
            'cpu_percent': process.cpu_percent()
        }

class URLQueueManager:
    """Efficient URL queue management with deduplication"""
    
    def __init__(self):
        self.queue = Queue()
        self.processed_urls = set()
        self.lock = Lock()
        
    def add_urls(self, urls, depth=0):
        """Add URLs to queue with deduplication"""
        added = 0
        with self.lock:
            for url in urls:
                if url not in self.processed_urls:
                    self.queue.put((url, depth))
                    self.processed_urls.add(url)
                    added += 1
        return added
    
    def get_next_url(self, timeout=1):
        """Get next URL from queue"""
        try:
            return self.queue.get(timeout=timeout)
        except Empty:
            return None
    
    def size(self):
        """Get queue size"""
        return self.queue.qsize()

class PerformanceMonitor:
    """Monitor and log performance metrics"""
    
    def __init__(self):
        self.start_time = None
        self.urls_processed = 0
        self.lock = Lock()
        
    def start(self):
        """Start performance monitoring"""
        self.start_time = time.time()
        perf_logger.info("Performance monitoring started")
        
    def record_url_processed(self):
        """Record URL processing"""
        with self.lock:
            self.urls_processed += 1
            
            if self.urls_processed % 10 == 0:
                elapsed = time.time() - self.start_time
                rate = self.urls_processed / elapsed
                perf_logger.info(f"Processed {self.urls_processed} URLs at {rate:.2f} URLs/sec")
    
    def get_stats(self):
        """Get performance statistics"""
        if not self.start_time:
            return {}
            
        elapsed = time.time() - self.start_time
        return {
            'urls_processed': self.urls_processed,
            'elapsed_seconds': elapsed,
            'urls_per_second': self.urls_processed / elapsed if elapsed > 0 else 0
        }

# Global instances
driver_pool = ChromeDriverPool()
memory_scraper = MemoryEfficientScraper()
url_manager = URLQueueManager()
perf_monitor = PerformanceMonitor()
