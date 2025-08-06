import logging
import time
import re
import json
import threading
from urllib.parse import urlparse, urljoin, parse_qs
from collections import defaultdict, deque
from datetime import datetime
import xml.etree.ElementTree as ET

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from bs4 import BeautifulSoup
import requests

logger = logging.getLogger('scraper')

class EnhancedWebCrawler:
    def __init__(self, base_url, max_depth=10, max_pages=1000):
        self.base_url = base_url
        self.base_netloc = urlparse(base_url).netloc
        self.max_depth = max_depth
        self.max_pages = max_pages
        
        # Data structures for comprehensive mapping
        self.site_map = defaultdict(dict)
        self.page_relationships = defaultdict(set)
        self.discovered_urls = set()
        self.crawled_urls = set()
        self.failed_urls = set()
        self.external_links = defaultdict(set)
        self.resource_links = defaultdict(set)
        
        # Enhanced crawling queues
        self.priority_queue = deque()  # High priority URLs
        self.normal_queue = deque()    # Normal priority URLs
        self.flood_fill_queue = deque() # URLs discovered through flood-fill
        
        # Statistics
        self.stats = {
            'total_pages': 0,
            'total_links': 0,
            'external_links': 0,
            'resources_found': 0,
            'forms_found': 0,
            'js_discovered': 0
        }
        
        self.driver = None
        self.setup_driver()
    
    def setup_driver(self):
        """Initialize Selenium WebDriver with enhanced capabilities"""
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        # Enable JavaScript execution logging
        options.add_experimental_option('prefs', {
            'profile.managed_default_content_settings.images': 2  # Disable images for speed
        })
        
        self.driver = webdriver.Chrome(options=options)
        self.driver.set_page_load_timeout(30)
    
    def discover_initial_urls(self):
        """Discover initial URLs through multiple sources"""
        discovered = set()
        
        # 1. Start with base URL
        discovered.add(self.base_url)
        
        # 2. Check for sitemap.xml
        sitemap_url = urljoin(self.base_url, '/sitemap.xml')
        try:
            response = requests.get(sitemap_url, timeout=10)
            if response.status_code == 200:
                discovered.update(self.parse_sitemap(response.text))
        except Exception as e:
            logger.info(f"No sitemap.xml found at {sitemap_url}")
        
        # 3. Check for robots.txt (for discovery, not restriction)
        robots_url = urljoin(self.base_url, '/robots.txt')
        try:
            response = requests.get(robots_url, timeout=10)
            if response.status_code == 200:
                discovered.update(self.parse_robots_txt(response.text))
        except Exception as e:
            logger.info(f"No robots.txt found at {robots_url}")
        
        # 4. Check common locations
        common_paths = [
            '/sitemap_index.xml', '/post-sitemap.xml', '/page-sitemap.xml',
            '/category-sitemap.xml', '/tag-sitemap.xml', '/author-sitemap.xml',
            '/rss', '/feed', '/rss.xml', '/atom.xml', '/feed.xml'
        ]
        
        for path in common_paths:
            full_url = urljoin(self.base_url, path)
            try:
                response = requests.head(full_url, timeout=5)
                if response.status_code == 200:
                    discovered.add(full_url)
            except:
                pass
        
        return discovered
    
    def parse_sitemap(self, sitemap_content):
        """Parse sitemap XML to extract URLs"""
        urls = set()
        try:
            root = ET.fromstring(sitemap_content)
            # Handle both sitemap and sitemapindex formats
            for url in root.iter():
                if url.tag.endswith('url') or url.tag.endswith('loc'):
                    if url.text and self.is_valid_url(url.text):
                        urls.add(url.text)
        except Exception as e:
            logger.error(f"Error parsing sitemap: {e}")
        return urls
    
    def parse_robots_txt(self, robots_content):
        """Parse robots.txt to discover sitemaps and allowed paths"""
        urls = set()
        lines = robots_content.split('\n')
        for line in lines:
            line = line.strip()
            if line.lower().startswith('sitemap:'):
                sitemap_url = line.split(':', 1)[1].strip()
                if self.is_valid_url(sitemap_url):
                    urls.add(sitemap_url)
        return urls
    
    def is_valid_url(self, url):
        """Check if URL is valid and belongs to the target domain"""
        try:
            parsed = urlparse(url)
            return (parsed.scheme in ('http', 'https') and 
                    parsed.netloc == self.base_netloc and
                    not any(ext in parsed.path.lower() for ext in ['.pdf', '.jpg', '.png', '.gif', '.css', '.js']))
        except:
            return False
    
    def flood_fill_discovery(self, current_url, depth):
        """Comprehensive page discovery using flood-fill techniques"""
        discovered = set()
        
        try:
            # Load page with enhanced JavaScript execution
            self.driver.get(current_url)
            time.sleep(2)
            
            # Execute comprehensive JavaScript discovery
            js_discovery_script = """
            var discovered = {
                links: [],
                forms: [],
                resources: [],
                dynamic_content: []
            };
            
            // Discover all anchor links
            var links = document.querySelectorAll('a[href]');
            for(var i = 0; i < links.length; i++) {
                discovered.links.push(links[i].href);
            }
            
            // Discover form actions
            var forms = document.querySelectorAll('form[action]');
            for(var i = 0; i < forms.length; i++) {
                discovered.forms.push(forms[i].action);
            }
            
            // Discover resources
            var resources = document.querySelectorAll('link[href], script[src], img[src]');
            for(var i = 0; i < resources.length; i++) {
                discovered.resources.push(resources[i].href || resources[i].src);
            }
            
            // Discover dynamic content URLs
            var elements = document.querySelectorAll('[data-url], [data-href], [data-link]');
            for(var i = 0; i < elements.length; i++) {
                var url = elements[i].getAttribute('data-url') || 
                         elements[i].getAttribute('data-href') || 
                         elements[i].getAttribute('data-link');
                if(url) discovered.dynamic_content.push(url);
            }
            
            // Discover JavaScript-generated links
            var scripts = document.querySelectorAll('script');
            for(var i = 0; i < scripts.length; i++) {
                var matches = scripts[i].textContent.match(/['"]([^'"]*\\.(html|php|asp|jsp))['"]/gi);
                if(matches) {
                    discovered.resources = discovered.resources.concat(matches.map(m => m.slice(1, -1)));
                }
            }
            
            return discovered;
            """
            
            discovered_data = self.driver.execute_script(js_discovery_script)
            
            # Process discovered URLs
            for url_list in discovered_data.values():
                for url in url_list:
                    full_url = urljoin(current_url, url)
                    if self.is_valid_url(full_url) and full_url not in self.crawled_urls:
                        discovered.add(full_url)
            
            # Enhanced link discovery through DOM traversal
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Discover links from various sources
            link_sources = [
                ('a', 'href'),
                ('link', 'href'),
                ('area', 'href'),
                ('base', 'href'),
                ('form', 'action'),
                ('iframe', 'src'),
                ('frame', 'src'),
                ('object', 'data'),
                ('embed', 'src')
            ]
            
            for tag, attr in link_sources:
                elements = soup.find_all(tag, attrs={attr: True})
                for element in elements:
                    url = element.get(attr)
                    if url:
                        full_url = urljoin(current_url, url)
                        if self.is_valid_url(full_url):
                            discovered.add(full_url)
            
            # Discover parameterized URLs
            current_parsed = urlparse(current_url)
            common_params = ['page', 'p', 'id', 'cat', 'tag', 'author', 's', 'search']
            for param in common_params:
                test_url = f"{current_url}{'&' if '?' in current_url else '?'}{param}=1"
                discovered.add(test_url)
            
            # Discover pagination
            pagination_selectors = ['.pagination a', '.pager a', '.page-numbers a', 'nav a']
            for selector in pagination_selectors:
                elements = soup.select(selector)
                for element in elements:
                    href = element.get('href')
                    if href:
                        full_url = urljoin(current_url, href)
                        if self.is_valid_url(full_url):
                            discovered.add(full_url)
            
        except Exception as e:
            logger.error(f"Error in flood-fill discovery for {current_url}: {e}")
        
        return discovered
    
    def analyze_page_structure(self, url, soup):
        """Analyze page structure and extract metadata"""
        page_data = {
            'url': url,
            'title': '',
            'meta_description': '',
            'headings': defaultdict(list),
            'links': [],
            'forms': [],
            'images': [],
            'scripts': [],
            'stylesheets': [],
            'response_time': 0,
            'status_code': 200
        }
        
        try:
            # Extract basic metadata
            if soup.title:
                page_data['title'] = soup.title.string.strip()
            
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc:
                page_data['meta_description'] = meta_desc.get('content', '')
            
            # Extract headings
            for level in range(1, 7):
                headings = soup.find_all(f'h{level}')
                for heading in headings:
                    text = heading.get_text(strip=True)
                    if text:
                        page_data['headings'][f'h{level}'].append(text)
            
            # Extract links
            for link in soup.find_all('a', href=True):
                href = link['href']
                text = link.get_text(strip=True)
                full_url = urljoin(url, href)
                page_data['links'].append({
                    'url': full_url,
                    'text': text,
                    'internal': self.is_valid_url(full_url)
                })
            
            # Extract forms
            for form in soup.find_all('form'):
                form_data = {
                    'action': form.get('action', ''),
                    'method': form.get('method', 'get'),
                    'inputs': []
                }
                for input_tag in form.find_all(['input', 'textarea', 'select']):
                    input_data = {
                        'type': input_tag.get('type', 'text'),
                        'name': input_tag.get('name', ''),
                        'id': input_tag.get('id', '')
                    }
                    form_data['inputs'].append(input_data)
                page_data['forms'].append(form_data)
            
            # Extract resources
            for img in soup.find_all('img', src=True):
                page_data['images'].append({
                    'src': urljoin(url, img['src']),
                    'alt': img.get('alt', '')
                })
            
            for script in soup.find_all('script', src=True):
                page_data['scripts'].append(urljoin(url, script['src']))
            
            for css in soup.find_all('link', rel='stylesheet', href=True):
                page_data['stylesheets'].append(urljoin(url, css['href']))
                
        except Exception as e:
            logger.error(f"Error analyzing page structure for {url}: {e}")
        
        return page_data
    
    def crawl_with_flood_fill(self):
        """Main crawling method with flood-fill discovery respecting robots.txt"""
        logger.info("Starting enhanced crawling with flood-fill discovery respecting robots.txt...")
        
        # Initialize robots.txt parser
        self.robots_parser = self.get_robot_parser(self.base_url)
        
        # Initial URL discovery
        initial_urls = self.discover_initial_urls()
        for url in initial_urls:
            if self.is_allowed_by_robots(url):
                self.normal_queue.append((url, 0))
                self.discovered_urls.add(url)
        
        crawl_count = 0
        
        while (self.priority_queue or self.normal_queue or self.flood_fill_queue) and crawl_count < self.max_pages:
            # Priority queue first, then normal, then flood-fill
            if self.priority_queue:
                current_url, depth = self.priority_queue.popleft()
            elif self.normal_queue:
                current_url, depth = self.normal_queue.popleft()
            elif self.flood_fill_queue:
                current_url, depth = self.flood_fill_queue.popleft()
            else:
                break
            
            if current_url in self.crawled_urls or depth > self.max_depth:
                continue
            
            # Check robots.txt before crawling
            if not self.is_allowed_by_robots(current_url):
                logger.info(f"Skipping {current_url} - disallowed by robots.txt")
                continue
            
            logger.info(f"Crawling: {current_url} (Depth: {depth})")
            
            try:
                start_time = time.time()
                self.driver.get(current_url)
                
                # Wait for page load
                WebDriverWait(self.driver, 10).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                
                # Scroll to load dynamic content
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                page_source = self.driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                
                # Analyze page structure
                page_data = self.analyze_page_structure(current_url, soup)
                self.site_map[current_url] = page_data
                
                # Extract content for storage
                content = self.extract_content(soup, current_url)
                
                # Flood-fill discovery
                new_urls = self.flood_fill_discovery(current_url, depth)
                
                # Add new URLs to appropriate queues
                for new_url in new_urls:
                    if new_url not in self.discovered_urls:
                        self.discovered_urls.add(new_url)
                        if depth < 3:  # Higher priority for shallow depths
                            self.priority_queue.append((new_url, depth + 1))
                        else:
                            self.flood_fill_queue.append((new_url, depth + 1))
                
                self.crawled_urls.add(current_url)
                crawl_count += 1
                
                # Update statistics
                self.stats['total_pages'] = len(self.crawled_urls)
                self.stats['total_links'] += len(page_data['links'])
                self.stats['external_links'] += len([l for l in page_data['links'] if not l['internal']])
                self.stats['resources_found'] += len(page_data['images']) + len(page_data['scripts']) + len(page_data['stylesheets'])
                self.stats['forms_found'] += len(page_data['forms'])
                
                logger.info(f"Successfully crawled {current_url} - Stats: {self.stats}")
                
            except Exception as e:
                logger.error(f"Error crawling {current_url}: {e}")
                self.failed_urls.add(current_url)
        
        logger.info(f"Crawling completed. Total pages: {len(self.crawled_urls)}")
        return self.generate_site_report()
    
    def extract_content(self, soup, url):
        """Enhanced content extraction"""
        content = []
        
        # Remove unwanted elements
        for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
            element.extract()
        
        # Extract content from various sources
        content_selectors = [
            'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
            'p', 'div', 'span', 'article', 'section',
            'li', 'td', 'th', 'blockquote'
        ]
        
        for selector in content_selectors:
            elements = soup.select(selector)
            for element in elements:
                text = element.get_text(strip=True)
                if text and len(text) > 10:
                    content.append({
                        'tag': selector,
                        'text': text,
                        'word_count': len(text.split())
                    })
        
        return content
    
    def generate_site_report(self):
        """Generate comprehensive site analysis report"""
        report = {
            'total_pages_crawled': len(self.crawled_urls),
            'total_failed_urls': len(self.failed_urls),
            'site_structure': dict(self.site_map),
            'external_links': dict(self.external_links),
            'resource_links': dict(self.resource_links),
            'statistics': self.stats,
            'crawl_path': list(self.crawled_urls),
            'discovered_urls': list(self.discovered_urls),
            'failed_urls': list(self.failed_urls)
        }
        
        # Generate sitemap
        sitemap = {
            'urls': list(self.crawled_urls),
            'structure': {},
            'hierarchy': self.build_hierarchy()
        }
        
        return {
            'report': report,
            'sitemap': sitemap,
            'site_map': dict(self.site_map)
        }
    
    def build_hierarchy(self):
        """Build URL hierarchy for better understanding"""
        hierarchy = defaultdict(list)
        
        for url in self.crawled_urls:
            parsed = urlparse(url)
            path_parts = parsed.path.strip('/').split('/')
            
            current_level = hierarchy
            for part in path_parts:
                if part:
                    if part not in current_level:
                        current_level[part] = {}
                    current_level = current_level[part]
        
        return dict(hierarchy)
    
    def get_robot_parser(self, base_url):
        """Initialize and return robots.txt parser"""
        from urllib.robotparser import RobotFileParser
        rp = RobotFileParser()
        robots_txt_url = urljoin(base_url, '/robots.txt')
        try:
            rp.set_url(robots_txt_url)
            rp.read()
            logger.info(f"Successfully loaded robots.txt from {robots_txt_url}")
            return rp
        except Exception as e:
            logger.warning(f"Error loading robots.txt from {robots_txt_url}: {e}")
            return None
    
    def is_allowed_by_robots(self, url):
        """Check if URL is allowed by robots.txt"""
        if not self.robots_parser:
            return True  # If no robots.txt, allow all
        
        try:
            user_agent = "EnhancedWebCrawler/1.0"
            return self.robots_parser.can_fetch(user_agent, url)
        except Exception as e:
            logger.warning(f"Error checking robots.txt for {url}: {e}")
            return True  # Allow on error
    
    def get_robots_disallowed_paths(self):
        """Get all disallowed paths from robots.txt for reporting"""
        if not self.robots_parser:
            return []
        
        disallowed = []
        try:
            # This is a simplified extraction - in practice, you might need to parse the robots.txt content directly
            # For now, we'll return a placeholder
            disallowed.append("robots.txt parsing enabled - disallowed paths will be skipped")
        except:
            pass
        return disallowed
    
    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logger.error(f"Error closing driver: {e}")

# Usage function
def run_enhanced_crawl(start_url, max_depth=10, max_pages=1000):
    """Run enhanced crawling with flood-fill discovery"""
    crawler = EnhancedWebCrawler(start_url, max_depth, max_pages)
    
    try:
        results = crawler.crawl_with_flood_fill()
        return results
    finally:
        crawler.cleanup()
