from django.db import models
from django.contrib.auth.models import User
import uuid

class ScrapingProject(models.Model):
    """Enhanced project model for organizing scraping tasks"""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    created_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='scraping_projects')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)
    
    class Meta:
        db_table = 'scraping_project'
        ordering = ['-created_at']
    
    def __str__(self):
        return self.name

class CrawlConfig(models.Model):
    """Configuration for crawling parameters"""
    project = models.OneToOneField(ScrapingProject, on_delete=models.CASCADE, related_name='config')
    max_depth = models.IntegerField(default=3)
    max_pages = models.IntegerField(default=1000)
    delay_between_requests = models.FloatField(default=1.0)
    user_agent = models.CharField(max_length=255, default='Mozilla/5.0')
    respect_robots_txt = models.BooleanField(default=True)
    enable_javascript = models.BooleanField(default=True)
    custom_headers = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'crawl_config'

class EnhancedMainURL(models.Model):
    """Enhanced main URL with additional metadata"""
    project = models.ForeignKey(ScrapingProject, on_delete=models.CASCADE, related_name='main_urls')
    url = models.URLField(max_length=500)
    title = models.CharField(max_length=500, blank=True)
    description = models.TextField(blank=True)
    total_words = models.IntegerField(default=0)
    status = models.CharField(max_length=20, default='pending')
    priority = models.IntegerField(default=5)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_crawled = models.DateTimeField(null=True, blank=True)
    crawl_duration = models.FloatField(default=0.0)
    error_message = models.TextField(blank=True)
    
    class Meta:
        db_table = 'enhanced_main_url'
        unique_together = ['project', 'url']
        ordering = ['-priority', 'created_at']
    
    def __str__(self):
        return f"{self.url} ({self.status})"

class CrawlSession(models.Model):
    """Track individual crawling sessions"""
    project = models.ForeignKey(ScrapingProject, on_delete=models.CASCADE, related_name='sessions')
    session_id = models.UUIDField(default=uuid.uuid4, editable=False)
    start_time = models.DateTimeField(auto_now_add=True)
    end_time = models.DateTimeField(null=True, blank=True)
    total_urls = models.IntegerField(default=0)
    processed_urls = models.IntegerField(default=0)
    successful_urls = models.IntegerField(default=0)
    failed_urls = models.IntegerField(default=0)
    status = models.CharField(max_length=20, default='running')
    settings_snapshot = models.JSONField(default=dict)
    
    class Meta:
        db_table = 'crawl_session'
        ordering = ['-start_time']
    
    def __str__(self):
        return f"Session {self.session_id} - {self.status}"

class ScrapedPage(models.Model):
    """Enhanced page content storage"""
    main_url = models.ForeignKey(EnhancedMainURL, on_delete=models.CASCADE, related_name='scraped_pages')
    session = models.ForeignKey(CrawlSession, on_delete=models.CASCADE, related_name='pages')
    url = models.URLField(max_length=500)
    title = models.CharField(max_length=500, blank=True)
    meta_description = models.TextField(blank=True)
    meta_keywords = models.TextField(blank=True)
    h1_tags = models.JSONField(default=list)
    h2_tags = models.JSONField(default=list)
    h3_tags = models.JSONField(default=list)
    content = models.TextField()
    word_count = models.IntegerField(default=0)
    images_count = models.IntegerField(default=0)
    links_count = models.IntegerField(default=0)
    internal_links = models.JSONField(default=list)
    external_links = models.JSONField(default=list)
    response_time = models.FloatField(default=0.0)
    status_code = models.IntegerField(default=200)
    content_hash = models.CharField(max_length=64, blank=True)
    scraped_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'scraped_page'
        unique_together = ['main_url', 'url']
        ordering = ['-scraped_at']
    
    def __str__(self):
        return f"{self.url} - {self.word_count} words"

class ScrapingProgress(models.Model):
    """Real-time progress tracking"""
    session = models.OneToOneField(CrawlSession, on_delete=models.CASCADE, related_name='progress')
    current_url = models.URLField(blank=True)
    current_depth = models.IntegerField(default=0)
    urls_found = models.IntegerField(default=0)
    urls_processed = models.IntegerField(default=0)
    estimated_completion = models.DateTimeField(null=True, blank=True)
    last_update = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'scraping_progress'

class ScrapingError(models.Model):
    """Store scraping errors for debugging"""
    session = models.ForeignKey(CrawlSession, on_delete=models.CASCADE, related_name='errors')
    url = models.URLField(max_length=500)
    error_type = models.CharField(max_length=50)
    error_message = models.TextField()
    traceback = models.TextField(blank=True)
    occurred_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'scraping_error'
        ordering = ['-occurred_at']
    
    def __str__(self):
        return f"{self.error_type} - {self.url}"

# Legacy model for backward compatibility
class ScrapedURL(models.Model):
    """Legacy scraped URL model for backward compatibility"""
    url = models.URLField(unique=True)
    title = models.CharField(max_length=500, blank=True)
    content = models.TextField()
    word_count = models.IntegerField(default=0)
    scraped_at = models.DateTimeField(auto_now_add=True)
    status_code = models.IntegerField(default=200)
    content_hash = models.CharField(max_length=64, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    
    class Meta:
        db_table = 'scraped_url'
        managed = False  # This is a legacy view
    
    def __str__(self):
        return self.url
