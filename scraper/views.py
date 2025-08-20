import os
import threading
import time
import logging
import json
from urllib.parse import urlparse, urljoin
from collections import defaultdict
from urllib.robotparser import RobotFileParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import multiprocessing
import random, time

from django.shortcuts import render, redirect
from django.http import HttpResponse, FileResponse, JsonResponse
from django.conf import settings
from django.utils.text import slugify

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import pandas as pd

# Setup logger
logger = logging.getLogger('scraper')
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global variables to store scraped data and common data flag
scraped_data = []
show_common_data = False

# Progress tracking variables
scrape_progress = {
    'status': 'idle',
    'total_urls': 0,
    'current_index': 0,
    'current_url': ''
}

USER_AGENT = "YourTranslationCrawler/1.0 "
MAX_CRAWL_DEPTH = 10
REQUEST_DELAY = time.sleep(random.uniform(0.5, 1.2))  # seconds
PAGE_LOAD_TIMEOUT = 18  # seconds
MAX_PAGE_SIZE = 10 * 1024 * 1024 # 10 MB

def create_driver():    
    options = Options()
    options.headless = True
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver

def is_valid_url(url, base_netloc):
    try:
        parsed = urlparse(url)
        return (parsed.scheme in ("http", "https")) and (parsed.netloc == base_netloc)
    except:
        return False

def get_robot_parser(base_url):
    rp = RobotFileParser()
    robots_txt_url = urljoin(base_url, '/robots.txt')
    try:
        rp.set_url(robots_txt_url)
        rp.read()
        logger.info(f"Fetched robots.txt from {robots_txt_url}")
        return rp
    except Exception as e:
        logger.warning(f"Error fetching/parsing robots.txt for {base_url}: {e}")
        return None

def get_page_name(soup):
    if soup.title:
        return soup.title.string.strip()
    return "No Title"

def extract_content(soup, url):
    content = []
    try:
        # Remove script and style tags
        for script_or_style in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
            script_or_style.extract()

        # Enhanced: Extract from more content types
        content_selectors = [
            'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
            'p', 
            'div[class*="content"]', 'div[class*="description"]', 'div[class*="text"]',
            'span[class*="content"]', 'span[class*="description"]', 'span[class*="text"]',
            'li[class*="feature"]', 'li[class*="spec"]', 'li[class*="detail"]',
            'td', 'th',
            '[class*="spec"]', '[class*="feature"]', '[class*="detail"]',
            '[id*="content"]', '[id*="description"]', '[id*="text"]'
        ]
        
        # Extract from specific selectors
        for selector in content_selectors:
            elements = soup.select(selector)
            for element in elements:
                text = element.get_text(strip=True)
                if text and len(text) > 10:  # Filter out very short text
                    # Determine appropriate tag name
                    tag_name = element.name or 'content'
                    if element.get('class'):
                        tag_name = f"{tag_name}_{'_'.join(element.get('class', []))}"
                    content.append((tag_name, text))

        # Also extract from meta descriptions
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            content.append(('meta_description', meta_desc['content']))
        
        # Extract from title
        if soup.title and soup.title.string:
            content.append(('title', soup.title.string.strip()))

        # Extract from alt text of images
        for img in soup.find_all('img', alt=True):
            if img.get('alt') and len(img['alt']) > 10:
                content.append(('image_alt', img['alt']))

        # Remove duplicates while preserving order
        seen = set()
        unique_content = []
        for tag, text in content:
            if text not in seen:
                seen.add(text)
                unique_content.append((tag, text))

        full_text = "\n".join([text for _, text in unique_content])
        if not full_text.strip() and len(str(soup)) > 500:
            logger.warning(f"Low text content extracted from {url} (possibly JS-heavy)")
            
        return unique_content
        
    except Exception as e:
        logger.error(f"Error extracting content from {url}: {e}")
        return []
                    
                    
def find_common_data(data):
    content_map = defaultdict(set)
    for entry in data:
        content_map[entry['Content']].add(entry['URL'])
    common_data = []
    total_urls = set(entry['URL'] for entry in data)
    for content_text, urls in content_map.items():
        # Show only data common to all URLs (no exceptions)
        if urls == total_urls:
            text = content_text.strip()
            # Improved word count: count characters for CJK, else split by whitespace
            if any('\u4e00' <= ch <= '\u9fff' for ch in text):
                word_count = len(text)
            else:
                word_count = len(text.split())
            common_data.append({
                'Content': content_text,
                'URLs': list(urls),
                'Word Count': word_count
            })
    return common_data

def filter_data_exclude_common(data, common_data):
    common_contents = set(item['Content'] for item in common_data)
    filtered = [entry for entry in data if entry['Content'] not in common_contents]
    return filtered

import re

def clean_sheet_name(name: str) -> str:
    # Remove invalid Excel characters
    name = re.sub(r'[\[\]\:\*\?\/\\]', '_', name)
    
    # Excel sheet names max length = 31 chars
    return name[:31]

def get_unique_sheet_name(base_name, existing_names):
    name = clean_sheet_name(base_name)
    original_name = name
    counter = 1
    # Ensure uniqueness
    while name.lower() in (n.lower() for n in existing_names):
        suffix = f"_{counter}"
        # Keep within Excel’s 31-char limit
        name = (original_name[:31 - len(suffix)]) + suffix
        counter += 1
    existing_names.add(name)
    return name


def save_to_excel(filename='scraped_data.xlsx'):
    global scraped_data, show_common_data
    filepath = os.path.join(settings.BASE_DIR, filename)
    if show_common_data:
        common_data = find_common_data(scraped_data)
        filtered_data = filter_data_exclude_common(scraped_data, common_data)

        # Group filtered_data by URL
        grouped = {}
        for entry in filtered_data:
            url = entry['URL']
            if url not in grouped:
                grouped[url] = []
            grouped[url].append(entry)

        with pd.ExcelWriter(filepath) as writer:
            summary = []
            existing_names = set()
            # 1. Create placeholder Summary first
            pd.DataFrame(columns=['URL', 'Total Words']).to_excel(writer, sheet_name='Summary', index=False)

            # 2. Write grouped URL sheets with unique names
            for url, entries in grouped.items():
                df = pd.DataFrame(entries)
                total_words = df['Word Count'].sum()
                raw_name = url.replace('http://', '').replace('https://', '').replace('/', '_')
                sheet_name = get_unique_sheet_name(raw_name, existing_names)
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                worksheet = writer.sheets[sheet_name]
                worksheet.write(len(df) + 1, 0, 'Total Words')
                worksheet.write(len(df) + 1, 4, total_words)
                summary.append({'URL': url, 'Total Words': total_words})

            # 3. Write common data sheet
            df_common = pd.DataFrame(common_data)
            df_common.to_excel(writer, sheet_name='Common Data', index=False)

            # 4. Overwrite Summary with final data
            df_summary = pd.DataFrame(summary)
            df_summary.to_excel(writer, sheet_name='Summary', index=False)
    else:
        # Group data by URL
        grouped = {}
        for entry in scraped_data:
            url = entry['URL']
            if url not in grouped:
                grouped[url] = []
            grouped[url].append(entry)

        with pd.ExcelWriter(filepath) as writer:
            summary = []

            # 1. Create empty summary first
            df_summary = pd.DataFrame(columns=['URL', 'Total Words'])
            df_summary.to_excel(writer, sheet_name='Summary', index=False)

            # 2. Write all URL sheets
            for url, entries in grouped.items():
                df = pd.DataFrame(entries)
                total_words = df['Word Count'].sum()
                raw_name = url.replace('http://', '').replace('https://', '').replace('/', '_')
                sheet_name = clean_sheet_name(raw_name)
                df.to_excel(writer, sheet_name=sheet_name, index=False)

                worksheet = writer.sheets[sheet_name]
                worksheet.write(len(df) + 1, 0, 'Total Words')
                worksheet.write(len(df) + 1, 4, total_words)
                summary.append({'URL': url, 'Total Words': total_words})

            # 3. Overwrite summary with final data
            df_summary = pd.DataFrame(summary)
            df_summary.to_excel(writer, sheet_name='Summary', index=False)


from django.shortcuts import redirect

import json
from django.http import JsonResponse

# Progress tracking variables
scrape_progress = {
    'status': 'idle',
    'total_urls': 0,
    'current_index': 0,
    'current_url': ''
}

def index(request):
    global show_common_data, scrape_progress
    if request.method == 'POST':
        url = request.POST.get('url')
        show_common_data = request.POST.get('show_common') == 'on'
        if url:
            scrape_progress['status'] = 'started'
            scrape_progress['total_urls'] = 0
            scrape_progress['current_index'] = 0
            scrape_progress['current_url'] = ''
            
            # Start scraping in background thread
            def scrape_and_save():
                crawl_site(url)
                save_to_excel()
                scrape_progress['status'] = 'completed'
            
            thread = threading.Thread(target=scrape_and_save)
            thread.start()
            
            # Return JSON response to indicate scraping started
            return JsonResponse({'status': 'started'})
    return render(request, 'scraper/index.html')

def discover_all_urls(start_url, driver):
    """Comprehensive URL discovery system to find ALL pages on a website"""
    base_netloc = urlparse(start_url).netloc
    discovered_urls = set()
    
    logger.info(f"Starting comprehensive URL discovery for {start_url}")
    
    # 1. Check for sitemap.xml
    try:
        sitemap_urls = [
            urljoin(start_url, '/sitemap.xml'),
            urljoin(start_url, '/sitemap_index.xml'),
            urljoin(start_url, '/sitemaps.xml'),
            urljoin(start_url, '/sitemap/sitemap.xml')
        ]
        
        for sitemap_url in sitemap_urls:
            try:
                driver.get(sitemap_url)
                if "404" not in driver.title and "not found" not in driver.title.lower():
                    soup = BeautifulSoup(driver.page_source, 'xml')
                    # Extract URLs from sitemap
                    for loc in soup.find_all('loc'):
                        url = loc.text.strip()
                        if is_valid_url(url, base_netloc):
                            discovered_urls.add(url)
                            logger.info(f"Found sitemap URL: {url}")
                    break
            except Exception as e:
                logger.debug(f"Sitemap {sitemap_url} not accessible: {e}")
                continue
    except Exception as e:
        logger.warning(f"Error checking sitemaps: {e}")
    
    # 2. Check robots.txt for sitemap references
    try:
        robots_url = urljoin(start_url, '/robots.txt')
        driver.get(robots_url)
        if "404" not in driver.title:
            robots_content = driver.page_source
            import re
            sitemap_matches = re.findall(r'Sitemap:\s*(.+)', robots_content, re.IGNORECASE)
            for sitemap_url in sitemap_matches:
                sitemap_url = sitemap_url.strip()
                try:
                    driver.get(sitemap_url)
                    soup = BeautifulSoup(driver.page_source, 'xml')
                    for loc in soup.find_all('loc'):
                        url = loc.text.strip()
                        if is_valid_url(url, base_netloc):
                            discovered_urls.add(url)
                            logger.info(f"Found robots.txt sitemap URL: {url}")
                except Exception as e:
                    logger.debug(f"Error processing robots.txt sitemap {sitemap_url}: {e}")
    except Exception as e:
        logger.warning(f"Error checking robots.txt: {e}")
    
    logger.info(f"Discovered {len(discovered_urls)} URLs from sitemaps and robots.txt")
    return discovered_urls

def comprehensive_crawl_site(start_url):
    """Comprehensive crawling system that finds and reads ALL pages"""
    global scraped_data, scrape_progress
    scraped_data = []
    visited = set()
    visited_lock = threading.Lock()
    data_lock = threading.Lock()
    progress_lock = threading.Lock()  # Add progress lock
    
    # Increase limits for comprehensive crawling
    max_workers = min(16, multiprocessing.cpu_count() * 2)  # Slightly reduced for stability
    max_urls = 1000  # Increased limit for comprehensive crawling
    max_depth = 15   # Increased depth
    logger.info(f"Using {max_workers} threads for crawling")
    logger.info(f"Starting comprehensive crawling with {max_workers} threads, max {max_urls} URLs, depth {max_depth}")
    
    base_netloc = urlparse(start_url).netloc
    url_queue = Queue()
    
    # Phase 1: Discover all URLs using sitemaps and systematic exploration
    initial_driver = create_driver()
    try:
        discovered_urls = discover_all_urls(start_url, initial_driver)
        
        # Add discovered URLs to queue
        for url in discovered_urls:
            url_queue.put((url, 0))
        
        # Always add the start URL
        url_queue.put((start_url, 0))
        
        logger.info(f"Added {len(discovered_urls) + 1} URLs to initial queue")
        
    finally:
        initial_driver.quit()
    
    # Initialize progress tracking
    with progress_lock:
        scrape_progress['status'] = 'running'
        scrape_progress['total_urls'] = url_queue.qsize()
        scrape_progress['current_index'] = 0
        scrape_progress['current_url'] = start_url

    def comprehensive_process_url(url_data):
        """Enhanced URL processing with comprehensive link discovery"""
        url, depth = url_data
        
        with visited_lock:
            if url in visited or depth > max_depth:
                return []
            visited.add(url)
        
        # Update progress with better tracking
        with progress_lock:
            current_processed = len(visited)
            scrape_progress['current_index'] = current_processed
            scrape_progress['current_url'] = url
            # Update total as we discover more URLs
            scrape_progress['total_urls'] = max(scrape_progress['total_urls'], current_processed + url_queue.qsize())
        
        driver = None
        new_urls = []
        
        try:
            driver = create_driver()
            driver.implicitly_wait(8)  # Slightly increased for comprehensive crawling
            
            logger.info(f"Comprehensively processing URL: {url} (Depth: {depth})")
            driver.get(url)
            
            # Enhanced page loading with multiple scroll attempts
            try:
                WebDriverWait(driver, 15).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                
                # Multiple scroll attempts to load all dynamic content
                for i in range(3):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                    driver.execute_script(f"window.scrollTo(0, {i * 500});")
                    time.sleep(1)
                
                # Scroll back to top
                driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(2)
                
            except Exception as e:
                logger.warning(f"Page loading issues for {url}: {e}")

            html = driver.page_source
            if len(html.encode('utf-8')) > MAX_PAGE_SIZE:
                logger.warning(f"Page size too large, skipping: {url}")
                return []

            soup = BeautifulSoup(html, 'html.parser')
            page_name = get_page_name(soup)
            content = extract_content(soup, url)
            
            # Thread-safe data addition
            with data_lock:
                for tag_name, text in content:
                    scraped_data.append({
                        'URL': url,
                        'Page Name': page_name,
                        'Heading/Tag': tag_name,
                        'Content': text,
                        'Word Count': len(text.split())
                    })
            
            # COMPREHENSIVE LINK DISCOVERY
            from selenium.webdriver.common.by import By
            from selenium.webdriver.common.action_chains import ActionChains
            
            # 1. Standard href links
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(url, href)
                if is_valid_url(full_url, base_netloc):
                    with visited_lock:
                        if full_url not in visited:
                            new_urls.append((full_url, depth + 1))
            
            # 2. Navigation menu links (comprehensive)
            nav_selectors = [
                'nav a[href]', 'header a[href]', 'footer a[href]',
                '.nav a[href]', '.navigation a[href]', '.menu a[href]',
                '.navbar a[href]', '.header a[href]', '.footer a[href]',
                '[class*="nav"] a[href]', '[class*="menu"] a[href]',
                '[id*="nav"] a[href]', '[id*="menu"] a[href]'
            ]
            
            for selector in nav_selectors:
                for link in soup.select(selector):
                    href = link.get('href')
                    if href:
                        full_url = urljoin(url, href)
                        if is_valid_url(full_url, base_netloc):
                            with visited_lock:
                                if full_url not in visited:
                                    new_urls.append((full_url, depth + 1))
            
            # 3. Language and region links (enhanced)
            try:
                # Look for language dropdowns and click them
                language_triggers = driver.find_elements(By.XPATH, 
                    "//a[contains(text(), 'Language')] | //button[contains(text(), 'Language')] | "
                    "//div[contains(@class, 'language')] | //div[contains(@class, 'lang')] | "
                    "//select[contains(@class, 'language')] | //select[contains(@class, 'lang')]"
                )
                
                for trigger in language_triggers:
                    try:
                        ActionChains(driver).move_to_element(trigger).perform()
                        time.sleep(2)
                        trigger.click()
                        time.sleep(3)
                        
                        # Find all links that appeared
                        lang_links = driver.find_elements(By.XPATH, "//a[@href]")
                        for link in lang_links:
                            try:
                                href = link.get_attribute('href')
                                if href and is_valid_url(href, base_netloc):
                                    with visited_lock:
                                        if href not in visited:
                                            new_urls.append((href, depth + 1))
                                            logger.info(f"Found language link: {href}")
                            except Exception:
                                continue
                        break
                    except Exception as e:
                        logger.debug(f"Error with language trigger: {e}")
                        continue
                        
            except Exception as e:
                logger.warning(f"Language detection failed: {e}")
            
            # 4. Pagination links
            pagination_selectors = [
                '.pagination a[href]', '.pager a[href]', '.page-numbers a[href]',
                '[class*="pagination"] a[href]', '[class*="pager"] a[href]',
                'a[href*="page="]', 'a[href*="p="]', 'a[href*="offset="]'
            ]
            
            for selector in pagination_selectors:
                for link in soup.select(selector):
                    href = link.get('href')
                    if href:
                        full_url = urljoin(url, href)
                        if is_valid_url(full_url, base_netloc):
                            with visited_lock:
                                if full_url not in visited:
                                    new_urls.append((full_url, depth + 1))
            
            # 5. Form actions and JavaScript links
            for form in soup.find_all('form', action=True):
                action = form.get('action')
                if action:
                    full_url = urljoin(url, action)
                    if is_valid_url(full_url, base_netloc):
                        with visited_lock:
                            if full_url not in visited:
                                new_urls.append((full_url, depth + 1))
            
            # 6. JavaScript onclick and data attributes
            import re
            for element in soup.find_all(['button', 'div', 'span', 'a']):
                # onclick handlers
                onclick = element.get('onclick', '')
                if onclick and 'location' in onclick:
                    href_match = re.search(r'["\']([^"\']+)["\']', onclick)
                    if href_match:
                        href = href_match.group(1)
                        full_url = urljoin(url, href)
                        if is_valid_url(full_url, base_netloc):
                            with visited_lock:
                                if full_url not in visited:
                                    new_urls.append((full_url, depth + 1))
                
                # data-href, data-url attributes
                for attr in ['data-href', 'data-url', 'data-link']:
                    data_url = element.get(attr)
                    if data_url:
                        full_url = urljoin(url, data_url)
                        if is_valid_url(full_url, base_netloc):
                            with visited_lock:
                                if full_url not in visited:
                                    new_urls.append((full_url, depth + 1))
            
            # 7. Try to interact with dropdowns and menus
            try:
                dropdowns = driver.find_elements(By.XPATH, 
                    "//select | //div[contains(@class, 'dropdown')] | "
                    "//ul[contains(@class, 'dropdown')] | //button[contains(@class, 'dropdown')]"
                )
                
                for dropdown in dropdowns[:5]:  # Limit to first 5 to avoid infinite loops
                    try:
                        ActionChains(driver).move_to_element(dropdown).perform()
                        time.sleep(1)
                        dropdown.click()
                        time.sleep(2)
                        
                        # Find new links that appeared
                        dropdown_links = driver.find_elements(By.XPATH, "//a[@href]")
                        for link in dropdown_links:
                            try:
                                href = link.get_attribute('href')
                                if href and is_valid_url(href, base_netloc):
                                    with visited_lock:
                                        if href not in visited:
                                            new_urls.append((href, depth + 1))
                            except Exception:
                                continue
                    except Exception:
                        continue
                        
            except Exception as e:
                logger.debug(f"Dropdown interaction failed: {e}")
            
            logger.info(f"Comprehensively processed {url}, found {len(new_urls)} new URLs")
            return new_urls
            
        except TimeoutException:
            logger.error(f"Timeout loading page {url}")
            return []
        except WebDriverException as e:
            logger.error(f"WebDriver error at {url}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
            return []
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

    # Main comprehensive crawling loop
    processed_count = 0
    
    while not url_queue.empty() and processed_count < max_urls:
        # Get batch of URLs to process
        current_batch = []
        batch_size = min(max_workers, url_queue.qsize())
        
        for _ in range(batch_size):
            if not url_queue.empty():
                current_batch.append(url_queue.get())
        
        if not current_batch:
            break
        
        # Update progress
        scrape_progress['total_urls'] = processed_count + len(current_batch) + url_queue.qsize()
        
        # Process URLs in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(comprehensive_process_url, url_data): url_data for url_data in current_batch}
            
            for future in as_completed(future_to_url):
                url_data = future_to_url[future]
                try:
                    new_urls = future.result()
                    processed_count += 1
                    
                    # Add new URLs to queue (with priority for shallow depths)
                    new_urls.sort(key=lambda x: x[1])  # Sort by depth
                    for new_url_data in new_urls:
                        if processed_count + url_queue.qsize() < max_urls:
                            url_queue.put(new_url_data)
                    
                except Exception as e:
                    logger.error(f"Error processing {url_data[0]}: {e}")
                    processed_count += 1
        
        # Small delay between batches
        time.sleep(0.2)
    
    logger.info(f"Comprehensive crawling completed. Processed {processed_count} URLs, collected {len(scraped_data)} content items")

# Use the comprehensive crawler
def crawl_site(start_url):
    return comprehensive_crawl_site(start_url)

def get_scrape_progress(request):
    global scrape_progress
    return JsonResponse(scrape_progress)

from django.utils.text import slugify

def view_data(request):
    global scraped_data, show_common_data
    if not scraped_data:
        return render(request, 'scraper/no_data.html')
    if show_common_data:
        common_data = find_common_data(scraped_data)
        filtered_data = filter_data_exclude_common(scraped_data, common_data)

        # Convert keys to match template variable names without spaces
        def convert_keys(data_list):
            new_list = []
            for item in data_list:
                new_item = {}
                for k, v in item.items():
                    new_key = k.replace(' ', '').replace('/', '')
                    new_item[new_key] = v
                new_list.append(new_item)
            return new_list

        common_data = convert_keys(common_data)
        filtered_data = convert_keys(filtered_data)

        # Group filtered_data by URL with total word count and slugify URLs
        grouped = {}
        url_headings = {}
        url_slugs = {}

        # Filter out entries with empty or missing 'URL'
        filtered_data = [entry for entry in filtered_data if entry.get('URL')]

        for entry in filtered_data:
            url = entry['URL']
            slug = slugify(url)
            if url not in grouped:
                grouped[url] = {'entries': [], 'total_words': 0}
                url_headings[url] = entry.get('PageName', url)
                url_slugs[url] = slug
            grouped[url]['entries'].append(entry)
            grouped[url]['total_words'] += entry['WordCount']
            
        # New grouping by language and type
        language_groups = {}
        type_groups = {}

        for entry in filtered_data:
            # Example: determine language from URL or content (simplified)
            url = entry['URL']
            content = entry['Content']
            # Simple heuristic: check for Japanese characters
            if any('\u3040' <= ch <= '\u30ff' for ch in content):
                lang = 'Japanese'
            elif any('\u4e00' <= ch <= '\u9fff' for ch in content):
                lang = 'Chinese'
            else:
                lang = 'Other'

            # Example type detection (placeholder, can be improved)
            if 'translation' in url.lower():
                doc_type = 'Translation'
            elif 'localization' in url.lower():
                doc_type = 'Localization'
            else:
                doc_type = 'Other'

            if lang not in language_groups:
                language_groups[lang] = set()
            language_groups[lang].add(url)

            if doc_type not in type_groups:
                type_groups[doc_type] = set()
            type_groups[doc_type].add(url)

        # Convert sets to sorted lists
        for k in language_groups:
            language_groups[k] = sorted(language_groups[k])
        for k in type_groups:
            type_groups[k] = sorted(type_groups[k])

        context = {
            'common_data': common_data,
            'grouped_filtered_data': grouped,
            'url_headings': url_headings,
            'url_slugs': url_slugs,
            'language_groups': language_groups,
            'type_groups': type_groups,
        }
        return render(request, 'scraper/view.html', context)
    else:
        # Convert keys in scraped_data as well
        def convert_keys(data_list):
            new_list = []
            for item in data_list:
                new_item = {}
                for k, v in item.items():
                    new_key = k.replace(' ', '').replace('/', '')
                    new_item[new_key] = v
                new_list.append(new_item)
            return new_list

        converted_data = convert_keys(scraped_data)

        # Group scraped_data by URL with total word count and slugify URLs
        grouped = {}
        url_headings = {}
        url_slugs = {}

        # Filter out entries with empty or missing 'URL'
        converted_data = [entry for entry in converted_data if entry.get('URL')]

        for entry in converted_data:
            url = entry['URL']
            slug = slugify(url)
            if url not in grouped:
                grouped[url] = {'entries': [], 'total_words': 0}
                url_headings[url] = entry.get('PageName', url)
                url_slugs[url] = slug
            grouped[url]['entries'].append(entry)
            grouped[url]['total_words'] += entry['WordCount']
        
        # New grouping by language and type
        language_groups = {}
        type_groups = {}

        for entry in converted_data:
            url = entry['URL']
            content = entry['Content']
            if any('\u3040' <= ch <= '\u30ff' for ch in content):
                lang = 'Japanese'
            elif any('\u4e00' <= ch <= '\u9fff' for ch in content):
                lang = 'Chinese'
            else:
                lang = 'Other'

            if 'translation' in url.lower():
                doc_type = 'Translation'
            elif 'localization' in url.lower():
                doc_type = 'Localization'
            else:
                doc_type = 'Other'

            if lang not in language_groups:
                language_groups[lang] = set()
            language_groups[lang].add(url)

            if doc_type not in type_groups:
                type_groups[doc_type] = set()
            type_groups[doc_type].add(url)

        for k in language_groups:
            language_groups[k] = sorted(language_groups[k])
        for k in type_groups:
            type_groups[k] = sorted(type_groups[k])

        context = {
            'grouped_scraped_data': grouped,
            'url_headings': url_headings,
            'url_slugs': url_slugs,
            'language_groups': language_groups,
            'type_groups': type_groups,
        }
        return render(request, 'scraper/view.html', context)

def download(request):
    filename = 'scraped_data.xlsx'
    filepath = os.path.join(settings.BASE_DIR, filename)
    if os.path.exists(filepath):
        return FileResponse(open(filepath, 'rb'), as_attachment=True)
    else:
        return HttpResponse("File not found", status=404)

from django.utils.text import slugify

def url_data(request, url_slug):
    global scraped_data
    # Find original URL from slug
    original_url = None
    for entry in scraped_data:
        if slugify(entry['URL']) == url_slug:
            original_url = entry['URL']
            break
    if not original_url:
        return render(request, 'scraper/no_data.html')

    # Filter scraped_data for this URL
    filtered_entries = [entry for entry in scraped_data if entry['URL'] == original_url]

    # Convert keys to match template variable names without spaces
    def convert_keys(data_list):
        new_list = []
        for item in data_list:
            new_item = {}
            for k, v in item.items():
                new_key = k.replace(' ', '').replace('/', '')
                new_item[new_key] = v
            new_list.append(new_item)
        return new_list

    converted_entries = convert_keys(filtered_entries)

    context = {
        'url': original_url,
        'entries': converted_entries,
    }
    return render(request, 'scraper/url_data.html', context)
