from celery import shared_task
from django.core.cache import cache
from django.utils import timezone
from .models_enhanced import (
    ScrapingProject, EnhancedMainURL, CrawlSession, 
    ScrapedPage, ScrapingProgress, ScrapingError, CrawlConfig
)
from .database_manager import DatabaseManager
from .async_scraper import AsyncWebScraper
import asyncio
import logging

logger = logging.getLogger(__name__)

@shared_task(bind=True, max_retries=3)
def async_scrape_project(self, project_id, main_url_ids=None):
    """
    Celery task for async scraping with progress tracking
    """
    try:
        project = ScrapingProject.objects.get(id=project_id)
        config = project.config
        
        # Create new crawl session
        session = CrawlSession.objects.create(
            project=project,
            settings_snapshot={
                'max_depth': config.max_depth,
                'max_pages': config.max_pages,
                'delay': config.delay_between_requests,
                'user_agent': config.user_agent,
                'enable_js': config.enable_javascript
            }
        )
        
        # Create progress tracking
        progress, created = ScrapingProgress.objects.get_or_create(
            session=session,
            defaults={'current_url': '', 'current_depth': 0}
        )
        
        # Get URLs to scrape
        if main_url_ids:
            urls = EnhancedMainURL.objects.filter(
                id__in=main_url_ids,
                project=project,
                status='pending'
            )
        else:
            urls = EnhancedMainURL.objects.filter(
                project=project,
                status='pending'
            )
        
        total_urls = urls.count()
        session.total_urls = total_urls
        session.save()
        
        # Initialize async scraper
        scraper = AsyncWebScraper(session, config)
        
        # Run async scraping
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            results = loop.run_until_complete(
                scraper.scrape_urls(urls)
            )
        finally:
            loop.close()
        
        # Update session completion
        session.end_time = timezone.now()
        session.status = 'completed'
        session.save()
        
        # Cache results for quick access
        cache_key = f"scraping_results_{session.id}"
        cache.set(cache_key, {
            'session_id': str(session.id),
            'total_processed': session.processed_urls,
            'successful': session.successful_urls,
            'failed': session.failed_urls,
            'completion_time': session.end_time.isoformat()
        }, timeout=3600)
        
        return {
            'session_id': str(session.id),
            'status': 'completed',
            'total_urls': total_urls,
            'successful': session.successful_urls,
            'failed': session.failed_urls
        }
        
    except Exception as e:
        logger.error(f"Scraping task failed: {str(e)}")
        
        if session:
            session.status = 'failed'
            session.end_time = timezone.now()
            session.save()
        
        # Retry with exponential backoff
        raise self.retry(exc=e, countdown=60 * (self.request.retries + 1))

@shared_task
def update_url_status(url_id, status, error_message=None):
    """Update URL status after processing"""
    try:
        url = EnhancedMainURL.objects.get(id=url_id)
        url.status = status
        if error_message:
            url.error_message = error_message
        url.last_crawled = timezone.now()
        url.save()
    except EnhancedMainURL.DoesNotExist:
        logger.warning(f"URL {url_id} not found for status update")

@shared_task
def cleanup_old_sessions():
    """Clean up old scraping sessions and data"""
    cutoff_date = timezone.now() - timezone.timedelta(days=30)
    
    old_sessions = CrawlSession.objects.filter(
        start_time__lt=cutoff_date,
        status='completed'
    )
    
    deleted_count = old_sessions.count()
    old_sessions.delete()
    
    logger.info(f"Cleaned up {deleted_count} old scraping sessions")
    
    return deleted_count

@shared_task
def retry_failed_urls(project_id, max_retries=3):
    """Retry failed URLs in a project"""
    project = ScrapingProject.objects.get(id=project_id)
    
    failed_urls = EnhancedMainURL.objects.filter(
        project=project,
        status='failed'
    )
    
    retry_count = 0
    for url in failed_urls:
        if url.error_message and 'timeout' in url.error_message.lower():
            url.status = 'pending'
            url.error_message = ''
            url.save()
            retry_count += 1
    
    if retry_count > 0:
        # Trigger new scraping task for retried URLs
        async_scrape_project.delay(project_id)
    
    return retry_count
