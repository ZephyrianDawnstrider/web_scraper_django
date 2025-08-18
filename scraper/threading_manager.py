import threading
import queue
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Callable, Any, Optional

class ThreadingManager:
    """Manages threading and concurrent execution for web scraping"""
    
    def __init__(self, max_workers=10):
        self.max_workers = max_workers
        self.logger = logging.getLogger(__name__)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._lock = threading.Lock()
        self._stats = {
            'processed': 0,
            'failed': 0,
            'total_time': 0,
            'start_time': time.time()
        }
    
    def process_urls_concurrent(self, urls: List[str], process_func: Callable, batch_size=50) -> List[Any]:
        """Process URLs concurrently with batching"""
        results = []
        
        # Process in batches to avoid overwhelming
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i+batch_size]
            batch_results = self._process_batch(batch, process_func)
            results.extend(batch_results)
        
        return results
    
    def _process_batch(self, urls: List[str], process_func: Callable) -> List[Any]:
        """Process a batch of URLs concurrently"""
        futures = []
        results = []
        
        for url in urls:
            future = self._executor.submit(process_func, url)
            futures.append(future)
        
        for future in as_completed(futures):
            try:
                result = future.result(timeout=30)
                if result:
                    results.append(result)
                    with self._lock:
                        self._stats['processed'] += 1
            except Exception as e:
                self.logger.error(f"Error processing URL: {e}")
                with self._lock:
                    self._stats['failed'] += 1
        
        return results
    
    def map_reduce(self, data: List[Any], map_func: Callable, reduce_func: Callable) -> Any:
        """Apply map-reduce pattern for data processing"""
        # Map phase
        map_results = []
        futures = [self._executor.submit(map_func, item) for item in data]
        
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    map_results.append(result)
            except Exception as e:
                self.logger.error(f"Map function error: {e}")
        
        # Reduce phase
        return reduce_func(map_results)
    
    def get_stats(self) -> dict:
        """Get current processing statistics"""
        with self._lock:
            stats = self._stats.copy()
            stats['elapsed_time'] = time.time() - stats['start_time']
            stats['avg_time_per_url'] = stats['total_time'] / max(stats['processed'], 1)
            return stats
    
    def shutdown(self):
        """Shutdown the thread pool"""
        self._executor.shutdown(wait=True)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

class ThreadSafeQueue:
    """Thread-safe queue for URL management"""
    
    def __init__(self, maxsize=0):
        self._queue = queue.Queue(maxsize=maxsize)
        self._lock = threading.Lock()
        self._size = 0
    
    def put(self, item):
        """Thread-safe put operation"""
        with self._lock:
            self._queue.put(item)
            self._size += 1
    
    def get(self, timeout=None):
        """Thread-safe get operation"""
        try:
            item = self._queue.get(timeout=timeout)
            with self._lock:
                self._size -= 1
            return item
        except queue.Empty:
            return None
    
    def empty(self):
        """Check if queue is empty"""
        return self._queue.empty()
    
    def qsize(self):
        """Get current queue size"""
        with self._lock:
            return self._size
    
    def clear(self):
        """Clear all items from queue"""
        with self._lock:
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                except queue.Empty:
                    break
