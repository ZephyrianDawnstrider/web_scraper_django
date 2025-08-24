from django.db import transaction
from django.core.exceptions import ValidationError
from .models import MainURL, SubURL, PageContent, ScrapingSession
from .database_manager import db_manager
import json

class DjangoDatabaseManager:
    """Django ORM wrapper for database operations"""
    
    @staticmethod
    def create_scraping_session(name: str, settings: dict = None) -> ScrapingSession:
        """Create a new scraping session"""
        return ScrapingSession.objects.create(
            session_name=name,
            settings=settings or {}
        )
    
    @staticmethod
    def add_main_url(url: str, session: ScrapingSession = None) -> MainURL:
        """Add a main URL to the database"""
        main_url, created = MainURL.objects.get_or_create(
            url=url,
            defaults={'session': session}
        )
        return main_url
    
    @staticmethod
    def add_sub_url(main_url: MainURL, sub_url: str, words: int = 0, status_code: int = None) -> SubURL:
        """Add a sub URL to the database"""
        sub_url_obj, created = SubURL.objects.get_or_create(
            main_url=main_url,
            sub_url=sub_url,
            defaults={
                'words': words,
                'status_code': status_code,
                'scraped_at': None
            }
        )
        return sub_url_obj
    
    @staticmethod
    def add_page_content(sub_url: SubURL, url: str, title: str, content: str, 
                        heading_tag: str = None, word_count: int = 0) -> PageContent:
        """Add page content to the database"""
        return PageContent.objects.create(
            sub_url=sub_url,
            url=url,
            title=title,
            content=content,
            heading_tag=heading_tag,
            word_count=word_count
        )
    
    @staticmethod
    def update_main_url_status(main_url: MainURL, status: str, error_message: str = None):
        """Update the status of a main URL"""
        main_url.status = status
        if error_message:
            main_url.error_message = error_message
        main_url.save()
    
    @staticmethod
    def update_sub_url_status(sub_url: SubURL, status_code: int):
        """Update the status code of a sub URL"""
        from django.utils import timezone
        sub_url.status_code = status_code
        sub_url.scraped_at = timezone.now()
        sub_url.save()
    
    @staticmethod
    def get_session_stats(session: ScrapingSession) -> dict:
        """Get statistics for a scraping session"""
        main_urls = MainURL.objects.filter(session=session)
        total_urls = main_urls.count()
        successful_urls = main_urls.filter(status='completed').count()
        failed_urls = main_urls.filter(status='failed').count()
        
        return {
            'total_urls': total_urls,
            'successful_urls': successful_urls,
            'failed_urls': failed_urls,
            'completion_rate': (successful_urls / total_urls * 100) if total_urls > 0 else 0
        }
    
    @staticmethod
    def get_all_content_for_url(main_url: MainURL) -> list:
        """Get all content for a main URL"""
        content_list = []
        sub_urls = SubURL.objects.filter(main_url=main_url)
        
        for sub_url in sub_urls:
            contents = PageContent.objects.filter(sub_url=sub_url)
            for content in contents:
                content_list.append({
                    'url': content.url,
                    'title': content.title,
                    'content': content.content,
                    'heading_tag': content.heading_tag,
                    'word_count': content.word_count
                })
        
        return content_list
    
    @staticmethod
    def search_content(query: str) -> list:
        """Search content across all pages"""
        from django.db.models import Q
        
        results = PageContent.objects.filter(
            Q(title__icontains=query) | Q(content__icontains=query)
        ).select_related('sub_url__main_url')
        
        return [
            {
                'url': result.url,
                'title': result.title,
                'content': result.content[:200] + '...' if len(result.content) > 200 else result.content,
                'main_url': result.sub_url.main_url.url
            }
            for result in results
        ]

class DatabaseManager:
    """Unified database manager that uses both Django ORM and raw SQL"""
    
    def __init__(self):
        self.django_manager = DjangoDatabaseManager()
        self.raw_manager = db_manager
    
    def save_scraped_data(self, main_url: str, sub_urls: list, session_name: str = None):
        """Save scraped data using both ORM and raw SQL for comparison"""
        
        # Create session if provided
        session = None
        if session_name:
            session = self.django_manager.create_scraping_session(session_name)
        
        # Add main URL
        main_url_obj = self.django_manager.add_main_url(main_url, session)
        
        # Add sub URLs and content
        for sub_url_data in sub_urls:
            sub_url_obj = self.django_manager.add_sub_url(
                main_url_obj,
                sub_url_data['url'],
                sub_url_data.get('words', 0),
                sub_url_data.get('status_code')
            )
            
            # Add page content
            for content_data in sub_url_data.get('contents', []):
                self.django_manager.add_page_content(
                    sub_url_obj,
                    content_data['url'],
                    content_data['title'],
                    content_data['content'],
                    content_data.get('heading_tag'),
                    content_data.get('word_count', 0)
                )
        
        return main_url_obj
    
    def get_scraping_summary(self, main_url: str) -> dict:
        """Get a summary of scraping results for a main URL"""
        try:
            main_url_obj = MainURL.objects.get(url=main_url)
            sub_urls = SubURL.objects.filter(main_url=main_url_obj)
            
            return {
                'main_url': main_url,
                'total_sub_urls': sub_urls.count(),
                'total_content_items': PageContent.objects.filter(
                    sub_url__main_url=main_url_obj
                ).count(),
                'sub_urls': [
                    {
                        'url': sub.sub_url,
                        'words': sub.words,
                        'status_code': sub.status_code,
                        'scraped_at': sub.scraped_at
                    }
                    for sub in sub_urls
                ]
            }
        except MainURL.DoesNotExist:
            return None
