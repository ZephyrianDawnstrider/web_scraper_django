import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, Optional
import time
from urllib.parse import urljoin, urlparse
import async_timeout

class AsyncHTTPClient:
    """Async HTTP client for concurrent web requests"""
    
    def __init__(self, max_connections=100, timeout=30):
        self.max_connections = max_connections
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
        self._session = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(
            limit=self.max_connections,
            limit_per_host=10,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=30
        )
        
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self._session:
            await self._session.close()
    
    async def fetch_url(self, url: str, method='GET', **kwargs) -> Optional[str]:
        """Fetch URL content asynchronously"""
        try:
            async with async_timeout.timeout(self.timeout):
                async with self._session.request(method, url, **kwargs) as response:
                    if response.status == 200:
                        content = await response.text()
                        return content
                    else:
                        self.logger.warning(f"HTTP {response.status} for {url}")
                        return None
        except asyncio.TimeoutError:
            self.logger.error(f"Timeout fetching {url}")
            return None
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return None
    
    async def fetch_multiple(self, urls: List[str], max_concurrent=50) -> Dict[str, Optional[str]]:
        """Fetch multiple URLs concurrently"""
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_one(url):
            async with semaphore:
                return await self.fetch_url(url)
        
        tasks = [fetch_one(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return dict(zip(urls, results))
    
    async def check_url_status(self, url: str) -> bool:
        """Check if URL is accessible"""
        try:
            async with async_timeout.timeout(10):
                async with self._session.head(url) as response:
                    return response.status == 200
        except:
            return False
    
    async def get_content_length(self, url: str) -> Optional[int]:
        """Get content length from headers"""
        try:
            async with self._session.head(url) as response:
                return response.headers.get('Content-Length')
        except:
            return None

class URLValidator:
    """URL validation and normalization utilities"""
    
    @staticmethod
    def is_valid_url(url: str) -> bool:
        """Check if URL is valid"""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False
    
    @staticmethod
    def normalize_url(url: str, base_url: str = None) -> str:
        """Normalize URL"""
        if base_url:
            url = urljoin(base_url, url)
        
        # Remove fragment
        url = url.split('#')[0]
        
        # Ensure consistent scheme
        if not url.startswith(('http://', 'https://')):
            url = 'http://' + url
        
        return url
    
    @staticmethod
    def extract_domain(url: str) -> str:
        """Extract domain from URL"""
        try:
            return urlparse(url).netloc
        except:
            return ""

class RateLimiter:
    """Rate limiting for requests"""
    
    def __init__(self, rate_limit=10, per_second=1):
        self.rate_limit = rate_limit
        self.per_second = per_second
        self._tokens = rate_limit
        self._last_update = time.time()
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        """Acquire token for making request"""
        async with self._lock:
            now = time.time()
            time_passed = now - self._last_update
            self._tokens = min(
                self.rate_limit,
                self._tokens + time_passed * (self.rate_limit / self.per_second)
            )
            self._last_update = now
            
            if self._tokens >= 1:
                self._tokens -= 1
                return True
            
            await asyncio.sleep((1 - self._tokens) / (self.rate_limit / self.per_second))
            self._tokens -= 1
            return True
