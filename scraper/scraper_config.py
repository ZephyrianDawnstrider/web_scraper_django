import os
from dataclasses import dataclass
from typing import List, Dict, Optional
import logging

@dataclass
class ScrapingConfig:
    """Configuration class for web scraper"""
    max_workers: int = 10
    max_depth: int = 10
    request_delay: float = 0.5
    page_load_timeout: int = 30
    max_page_size: int = 10 * 1024 * 1024  # 10MB
    user_agent: str = "EnhancedWebScraper/2.0"
    max_urls: int = 1000
    retry_attempts: int = 3
    retry_delay: float = 2.0
    cache_ttl: int = 3600  # 1 hour
    rate_limit: int = 10  # requests per second
    enable_javascript: bool = True
    enable_images: bool = False
    output_format: str = "excel"
    output_filename: str = "scraped_data"
    
    # Content filtering
    min_word_count: int = 5
    max_word_count: int = 10000
    exclude_patterns: List[str] = None
    include_patterns: List[str] = None
    
    # Selectors for content extraction
    content_selectors: List[str] = None
    
    def __post_init__(self):
        if self.exclude_patterns is None:
            self.exclude_patterns = [
                r'\.(css|js|png|jpg|jpeg|gif|svg|ico)$',
                r'/wp-admin/',
                r'/admin/',
                r'login',
                r'signup',
                r'register'
            ]
        
        if self.include_patterns is None:
            self.include_patterns = [
                r'/product/',
                r'/service/',
                r'/about',
                r'/contact',
                r'/features',
                r'/pricing'
            ]
            
        if self.content_selectors is None:
            self.content_selectors = [
                'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
                'p', 'div[class*="content"]', 'div[class*="description"]',
                'span[class*="content"]', 'li', 'td', 'th',
                '[class*="spec"]', '[class*="feature"]', '[class*="detail"]'
            ]

class ConfigManager:
    """Manages configuration loading and validation"""
    
    @staticmethod
    def load_from_env() -> ScrapingConfig:
        """Load configuration from environment variables"""
        config = ScrapingConfig()
        
        # Override with env vars if available
        config.max_workers = int(os.getenv('MAX_WORKERS', config.max_workers))
        config.max_depth = int(os.getenv('MAX_DEPTH', config.max_depth))
        config.request_delay = float(os.getenv('REQUEST_DELAY', config.request_delay))
        config.max_urls = int(os.getenv('MAX_URLS', config.max_urls))
        config.enable_javascript = os.getenv('ENABLE_JAVASCRIPT', 'true').lower() == 'true'
        
        return config
    
    @staticmethod
    def validate_config(config: ScrapingConfig) -> bool:
        """Validate configuration parameters"""
        if config.max_workers < 1 or config.max_workers > 50:
            logging.error("max_workers must be between 1 and 50")
            return False
            
        if config.max_depth < 1 or config.max_depth > 20:
            logging.error("max_depth must be between 1 and 20")
            return False
            
        if config.request_delay < 0.1:
            logging.error("request_delay must be at least 0.1 seconds")
            return False
            
        return True
