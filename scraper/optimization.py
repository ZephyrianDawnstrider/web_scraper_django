"""
Performance optimization utilities for web scraper
Handles bulk operations, memory management, and database optimization
"""
from django.db import transaction
from django.core.cache import cache
from scraper.models import ScrapedPage, EnhancedMainURL, CrawlSession
import gc
import logging

logger = logging.getLogger(__name__)

class BulkPageProcessor:
    """Efficient bulk processing for scraped pages"""
    
    def __init__(self, batch_size=100):
        self.batch_size = batch_size
        self.page_buffer = []
        self.processed_count = 0
        
    def add_page(self, page_data):
        """Add page to buffer for bulk processing"""
        self.page_buffer.append(page_data)
        
        if len(self.page_buffer) >= self.batch_size:
            self._flush_buffer()
    
    def _flush_buffer(self):
        """Save buffered pages to database in bulk"""
        if not self.page_buffer:
            return
            
        try:
            with transaction.atomic():
                # Use bulk_create for maximum performance
                pages_to_create = []
                
                for page_data in self.page_buffer:
                    page = ScrapedPage(**page_data)
                    pages_to_create.append(page)
                
                ScrapedPage.objects.bulk_create(
                    pages_to_create,
                    batch_size=self.batch_size,
                    ignore_conflicts=True
                )
                
                self.processed_count += len(pages_to_create)
                logger.info(f"Bulk saved {len(pages_to_create)} pages")
                
                # Clear buffer and force garbage collection
                self.page_buffer.clear()
                gc.collect()
                
        except Exception as e:
            logger.error(f"Error in bulk save: {e}")
            # Fallback to individual saves
            self._fallback_individual_saves()
    
    def _fallback_individual_saves(self):
        """Fallback to individual saves if bulk fails"""
        for page_data in self.page_buffer:
            try:
                ScrapedPage.objects.create(**page_data)
                self.processed_count += 1
            except Exception as e:
                logger.error(f"Error saving individual page: {e}")
        
        self.page_buffer.clear()
    
    def finish(self):
        """Process remaining pages in buffer"""
        if self.page_buffer:
            self._flush_buffer()
        
        return self.processed_count

class URLCache:
    """Cache for URL deduplication and processing status"""
    
    def __init__(self, session_id):
        self.session_id = str(session_id)
        self.cache_key = f"url_cache_{session_id}"
        
    def is_processed(self, url):
        """Check if URL has been processed"""
        return cache.get(f"{self.cache_key}_{url}") is not None
    
    def mark_processed(self, url):
        """Mark URL as processed"""
        cache.set(f"{self.cache_key}_{url}", True, timeout=3600)
    
    def get_processed_count(self):
        """Get count of processed URLs"""
        # This is a simplified implementation
        # In production, use Redis sets or similar
        return 0
    
    def clear(self):
        """Clear cache for session"""
        # In production, use cache pattern matching
        pass

class MemoryManager:
    """Manage memory usage during large crawls"""
    
    def __init__(self, max_memory_mb=500):
        self.max_memory_mb = max_memory_mb
        self.check_interval = 50  # Check every 50 pages
        
    def should_gc(self, processed_count):
        """Check if garbage collection should be triggered"""
        return processed_count % self.check_interval == 0
    
    def cleanup(self):
        """Force garbage collection and memory cleanup"""
        gc.collect()
        logger.debug("Memory cleanup completed")

class ProgressTracker:
    """Efficient progress tracking without global variables"""
    
    def __init__(self, session_id):
        self.session_id = session_id
        self.session = CrawlSession.objects.get(id=session_id)
        
    def update_progress(self, current_url=None, processed=0, found=0):
        """Update progress in database"""
        try:
            progress, created = self.session.progress.get_or_create(
                defaults={
                    'current_url': current_url or '',
                    'urls_processed': processed,
                    'urls_found': found
                }
            )
            
            if not created:
                progress.current_url = current_url or progress.current_url
                progress.urls_processed += processed
                progress.urls_found += found
                progress.save()
                
        except Exception as e:
            logger.error(f"Error updating progress: {e}")
    
    def get_progress(self):
        """Get current progress"""
        try:
            progress = self.session.progress.first()
            if progress:
                return {
                    'current_url': progress.current_url,
                    'processed': progress.urls_processed,
                    'found': progress.urls_found,
                    'total': self.session.total_urls
                }
        except:
            pass
        return {'processed': 0, 'found': 0, 'total': 0}

class QueryOptimizer:
    """Optimize database queries for better performance"""
    
    @staticmethod
    def get_optimized_pages(session_id):
        """Get pages with optimized query"""
        return ScrapedPage.objects.filter(
            session_id=session_id
        ).select_related(
            'main_url'
        ).only(
            'id', 'url', 'title', 'word_count', 'scraped_at'
        )
    
    @staticmethod
    def get_url_stats(session_id):
        """Get URL processing statistics"""
        return EnhancedMainURL.objects.filter(
            project__sessions__id=session_id
        ).aggregate(
            total=models.Count('id'),
            pending=models.Count('id', filter=models.Q(status='pending')),
            processed=models.Count('id', filter=models.Q(status='completed')),
            failed=models.Count('id', filter=models.Q(status='failed'))
        )
