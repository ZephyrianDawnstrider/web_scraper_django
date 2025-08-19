import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import logging

logger = logging.getLogger(__name__)

class AsyncScraper:
    """Async scraper for non-Selenium operations"""
    
    def __init__(self, max_concurrent=50):
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
    async def fetch_url_async(self, session, url):
        """Async HTTP fetch for non-Selenium content"""
        async with self.semaphore:
            try:
                async with session.get(url, timeout=30) as response:
                    content = await response.text()
                    return {
                        'url': url,
                        'status': response.status,
                        'content_length': len(content),
                        'content': content[:1000]  # Limit content size
                    }
            except Exception as e:
                logger.error(f"Async fetch error for {url}: {e}")
                return None
    
    async def scrape_urls_async(self, urls):
        """Async scraping for multiple URLs"""
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_url_async(session, url) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out None results and exceptions
            valid_results = [r for r in results if r and isinstance(r, dict)]
            return valid_results
    
    def run_async_scraping(self, urls):
        """Run async scraping from sync context"""
        return asyncio.run(self.scrape_urls_async(urls))
