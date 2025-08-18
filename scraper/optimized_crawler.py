import asyncio
import logging
import time
import threading
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

from .driver_pool import WebDriverPool
from .threading_manager import ThreadingManager
from .async_requests import AsyncHTTPClient, URLValidator

class OptimizedWebCrawler:
    """High-performance web crawler with threading and Selenium optimization"""
    
    def __init__(self, max_workers=10, max_drivers=5, headless=True):
        self.max_workers = max_workers
        self.max_drivers = max_drivers
        self.headless = headless
        
        # Initialize components
        self.driver_pool = WebDriverPool(max_drivers=max_drivers, headless=headless)
        self.threading_manager = ThreadingManager(max_workers=max_workers)
        self.url_validator = URLValidator()
        
        # Thread-safe data structures
        self._visited_urls = set()
        self._lock = threading.Lock()
        self._stats = {
            'urls_processed': 0,
            'pages_scraped': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        self.logger = logging.getLogger(__name__)
        
    def crawl_site(self, start_url: str, max_pages=100, max_depth=3) -> Dict[str, Any]:
        """Main crawling function with performance optimizations"""
        self.logger.info(f"Starting optimized crawl of {start_url}")
        
        # Validate and normalize start URL
        if not self.url_validator.is_valid_url(start_url):
            raise ValueError("Invalid start URL")
        
        start_url = self.url_validator.normalize_url(start_url)
        domain = self.url_validator.extract_domain(start_url)
        
        # Initialize crawl data
        crawl_data = {
            'site_map': {},
            'relationships': [],
            'metadata': {},
            'errors': []
        }
        
        # Use threading manager for concurrent processing
        with self.threading_manager as tm:
            # Process URLs in batches
            urls_to_process = [(start_url, 0)]  # (url, depth)
            processed_urls = set()
            
            while urls_to_process and len(processed_urls) < max_pages:
                batch = urls_to_process[:min(50, len(urls_to_process))]
                urls_to_process = urls_to_process[len(batch):]
                
                # Process batch concurrently
                batch_results = tm.process_urls_concurrent(
                    [url for url, _ in batch],
                    lambda url: self._process_single_page(url, domain, max_depth)
                )
                
                # Process results and discover new URLs
                for result in batch_results:
                    if result and result['success']:
                        url = result['url']
                        processed_urls.add(url)
                        
                        # Add to site map
                        crawl_data['site_map'][url] = result['data']
                        
                        # Add relationships
                        for link in result['data'].get('links', []):
                            crawl_data['relationships'].append({
                                'from': url,
                                'to': link,
                                'type': 'link'
                            })
                        
                        # Discover new URLs for next iteration
                        new_urls = self._discover_new_urls(
                            result['data'].get('links', []),
                            domain,
                            result['depth'] + 1,
                            max_depth
                        )
                        
                        for new_url in new_urls:
                            if new_url not in processed_urls:
                                urls_to_process.append((new_url, result['depth'] + 1))
                
                # Update stats
                with self._lock:
                    self._stats['urls_processed'] += len(batch)
        
        # Add metadata
        crawl_data['metadata'] = {
            'total_pages': len(crawl_data['site_map']),
            'total_links': len(crawl_data['relationships']),
            'crawl_time': time.time() - self._stats['start_time'],
            'errors': self._stats['errors']
        }
        
        return crawl_data
    
    def _process_single_page(self, url: str, domain: str, max_depth: int) -> Optional[Dict[str, Any]]:
        """Process a single page with optimized settings"""
        try:
            # Get driver from pool
            driver = self.driver_pool.get_driver()
            if not driver:
                return None
            
            try:
                # Navigate to page
                driver.get(url)
                
                # Extract page data
                page_data = {
                    'url': url,
                    'title': self._extract_title(driver),
                    'meta_description': self._extract_meta_description(driver),
                    'headings': self._extract_headings(driver),
                    'links': self._extract_links(driver, domain),
                    'images': self._extract_images(driver),
                    'text_content': self._extract_text_content(driver),
                    'load_time': time.time(),
                    'depth': 0  # Will be set by caller
                }
                
                # Return driver to pool
                self.driver_pool.return_driver(driver)
                
                return {
                    'success': True,
                    'url': url,
                    'data': page_data,
                    'depth': 0
                }
                
            except Exception as e:
                self.driver_pool.return_driver(driver)
                raise e
                
        except Exception as e:
            self.logger.error(f"Error processing {url}: {e}")
            with self._lock:
                self._stats['errors'] += 1
            return None
    
    def _discover_new_urls(self, links: List[str], domain: str, current_depth: int, max_depth: int) -> List[str]:
        """Discover new URLs for crawling with deduplication"""
        if current_depth >= max_depth:
            return []
        
        new_urls = []
        for link in links:
            normalized = self.url_validator.normalize_url(link)
            if self.url_validator.extract_domain(normalized) == domain:
                with self._lock:
                    if normalized not in self._visited_urls:
                        self._visited_urls.add(normalized)
                        new_urls.append(normalized)
        
        return new_urls
    
    def _extract_title(self, driver) -> str:
        """Extract page title"""
        try:
            return driver.execute_script("return document.title") or ""
        except:
            return ""
    
    def _extract_meta_description(self, driver) -> str:
        """Extract meta description"""
        try:
            meta = driver.execute_script(
                "return document.querySelector('meta[name=\"description\"]')?.content || ''"
            )
            return meta
        except:
            return ""
    
    def _extract_headings(self, driver) -> Dict[str, List[str]]:
        """Extract headings (h1-h6)"""
        headings = {}
        try:
            for level in range(1, 7):
                h_tags = driver.execute_script(
                    f"return Array.from(document.querySelectorAll('h{level}')).map(h => h.textContent.trim())"
                )
                if h_tags:
                    headings[f'h{level}'] = h_tags
        except:
            pass
        return headings
    
    def _extract_links(self, driver, domain: str) -> List[str]:
        """Extract internal links"""
        links = []
        try:
            hrefs = driver.execute_script(
                "return Array.from(document.querySelectorAll('a[href]')).map(a => a.href)"
            )
            for href in hrefs:
                if self.url_validator.extract_domain(href) == domain:
                    links.append(href)
        except:
            pass
        return links
    
    def _extract_images(self, driver) -> List[str]:
        """Extract image URLs"""
        try:
            return driver.execute_script(
                "return Array.from(document.querySelectorAll('img[src]')).map(img => img.src)"
            )
        except:
            return []
    
    def _extract_text_content(self, driver) -> str:
        """Extract main text content"""
        try:
            return driver.execute_script(
                "return document.body ? document.body.innerText.trim() : ''"
            )
        except:
            return ""
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        with self._lock:
            stats = self._stats.copy()
            stats['elapsed_time'] = time.time() - stats['start_time']
            stats['pages_per_second'] = stats['urls_processed'] / max(stats['elapsed_time'], 1)
            return stats
    
    def close(self):
        """Clean up resources"""
        self.driver_pool.close_all()
        self.threading_manager.shutdown()
