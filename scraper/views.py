import os
import threading
import time
import logging
from urllib.parse import urlparse, urljoin
from collections import defaultdict
from urllib.robotparser import RobotFileParser

from django.shortcuts import render, redirect
from django.http import HttpResponse, FileResponse
from django.conf import settings

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
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

USER_AGENT = "YourTranslationCrawler/1.0 (info@yourcompany.com)"
MAX_CRAWL_DEPTH = 10
REQUEST_DELAY = 0.25  # seconds
PAGE_LOAD_TIMEOUT = 30  # seconds
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
                    
def crawl_site(start_url):
    global scraped_data
    scraped_data = []
    visited = set()
    to_visit = [(start_url, 0)]  # tuple of (url, depth)
    base_netloc = urlparse(start_url).netloc

    # Bypass robots.txt completely by not using robot_parser
    driver = create_driver()

    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import random

    def safe_click(element):
        try:
            element.click()
            time.sleep(2)
            return True
        except Exception as e:
            logger.warning(f"Failed to click element: {e}")
            return False

    def fill_and_submit_form(form):
        try:
            inputs = form.find_elements(By.TAG_NAME, 'input')
            for input_element in inputs:
                input_type = input_element.get_attribute('type')
                if input_type in ['text', 'search']:
                    input_element.clear()
                    input_element.send_keys('Test')
                elif input_type == 'email':
                    input_element.clear()
                    input_element.send_keys('test@example.com')
                elif input_type == 'tel':
                    input_element.clear()
                    input_element.send_keys('1234567890')
                elif input_type == 'number':
                    input_element.clear()
                    input_element.send_keys('123')
                elif input_type == 'password':
                    input_element.clear()
                    input_element.send_keys('password')
                elif input_type == 'url':
                    input_element.clear()
                    input_element.send_keys('http://example.com')
                # Add more input types as needed

            textareas = form.find_elements(By.TAG_NAME, 'textarea')
            for textarea in textareas:
                textarea.clear()
                textarea.send_keys('Test message')

            selects = form.find_elements(By.TAG_NAME, 'select')
            for select in selects:
                options = select.find_elements(By.TAG_NAME, 'option')
                if options:
                    options[0].click()

            # Submit the form
            form.submit()
            time.sleep(3)
            return True
        except Exception as e:
            logger.warning(f"Failed to fill and submit form: {e}")
            return False

    while to_visit:
        url, depth = to_visit.pop(0)
        if url in visited:
            continue
        if depth > MAX_CRAWL_DEPTH:
            logger.info(f"Skipping {url} due to max crawl depth {MAX_CRAWL_DEPTH}")
            continue

        try:
            driver.get(url)
            time.sleep(REQUEST_DELAY)  # rate limiting delay

            # Enhanced: Wait for dynamic content
            try:
                WebDriverWait(driver, 15).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)
                driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(1)
            except:
                pass

            # Interact with language dropdown menu if present
            try:
                # Attempt to find language dropdown by common selectors
                lang_dropdown = None
                possible_selectors = [
                    "//a[contains(text(), 'Language')]",
                    "//div[contains(@class, 'language')]",
                    "//div[contains(@id, 'language')]",
                    "//ul[contains(@class, 'language')]",
                    "//nav[contains(@class, 'language')]",
                    "//button[contains(text(), 'Language')]"
                ]
                for selector in possible_selectors:
                    elements = driver.find_elements(By.XPATH, selector)
                    if elements:
                        lang_dropdown = elements[0]
                        break
                if lang_dropdown:
                    ActionChains(driver).move_to_element(lang_dropdown).perform()
                    time.sleep(2)
                    # Extract links inside dropdown
                    dropdown_links = lang_dropdown.find_elements(By.XPATH, ".//following-sibling::ul//a[@href]")
                    for link in dropdown_links:
                        href = link.get_attribute('href')
                        if href and is_valid_url(href, base_netloc) and href not in visited:
                            logger.info(f"Adding language dropdown URL: {href}")
                            to_visit.append((href, depth + 1))
            except Exception as e:
                logger.warning(f"Language dropdown interaction failed: {e}")

            html = driver.page_source
            if len(html.encode('utf-8')) > MAX_PAGE_SIZE:
                logger.warning(f"Page size too large, skipping content extraction for {url}")
                visited.add(url)
                continue

            soup = BeautifulSoup(html, 'html.parser')
            page_name = get_page_name(soup)
            content = extract_content(soup, url)
            for tag_name, text in content:
                scraped_data.append({
                    'URL': url,
                    'Page Name': page_name,
                    'Heading/Tag': tag_name,
                    'Content': text,
                    'Word Count': len(text.split())
                })
            logger.info(f"Successfully crawled {url} (Depth: {depth})")
            visited.add(url)

            # Interact with buttons
            try:
                buttons = driver.find_elements(By.TAG_NAME, 'button')
                for button in buttons:
                    try:
                        button_text = button.text.strip()
                        if button_text and button.is_displayed() and button.is_enabled():
                            if safe_click(button):
                                new_url = driver.current_url
                                if is_valid_url(new_url, base_netloc) and new_url not in visited:
                                    logger.info(f"Adding URL from button click: {new_url}")
                                    to_visit.append((new_url, depth + 1))
                                driver.back()
                                time.sleep(2)
                    except Exception as e:
                        logger.warning(f"Error interacting with button: {e}")
            except Exception as e:
                logger.warning(f"Button interaction failed: {e}")

            # Interact with forms
            try:
                forms = driver.find_elements(By.TAG_NAME, 'form')
                for form in forms:
                    if fill_and_submit_form(form):
                        new_url = driver.current_url
                        if is_valid_url(new_url, base_netloc) and new_url not in visited:
                            logger.info(f"Adding URL from form submission: {new_url}")
                            to_visit.append((new_url, depth + 1))
                        driver.back()
                        time.sleep(2)
            except Exception as e:
                logger.warning(f"Form interaction failed: {e}")

            # Enhanced: Find more types of links
            new_links = []

            # Standard href links
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(url, href)
                if is_valid_url(full_url, base_netloc) and full_url not in visited:
                    new_links.append((full_url, depth + 1))

            # Button links with onclick
            for button in soup.find_all(['button', 'div', 'span']):
                onclick = button.get('onclick', '')
                if onclick and 'location.href' in onclick:
                    import re
                    href_match = re.search(r'location\.href\s*=\s*["\']([^"\']+)["\']', onclick)
                    if href_match:
                        href = href_match.group(1)
                        full_url = urljoin(url, href)
                        if is_valid_url(full_url, base_netloc) and full_url not in visited:
                            new_links.append((full_url, depth + 1))

            # Data attributes that might contain URLs
            for element in soup.find_all(attrs={'data-url': True}):
                href = element.get('data-url')
                full_url = urljoin(url, href)
                if is_valid_url(full_url, base_netloc) and full_url not in visited:
                    new_links.append((full_url, depth + 1))

            # Add new unique links
            for new_url, new_depth in new_links:
                if new_url not in visited and all(new_url != u for u, _ in to_visit):
                    logger.info(f"Adding URL to crawl queue: {new_url} (Depth: {new_depth})")
                    to_visit.append((new_url, new_depth))

        except TimeoutException:
            logger.error(f"Timeout loading page {url}")
            visited.add(url)
        except WebDriverException as e:
            logger.error(f"WebDriver error at {url}: {e}")
            # Attempt to recover from session errors by restarting driver
            if "invalid session id" in str(e).lower() or "session deleted" in str(e).lower():
                logger.info("Restarting WebDriver due to session error")
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = create_driver()
                # Re-queue the current URL to retry
                to_visit.insert(0, (url, depth))
            else:
                visited.add(url)
        except Exception as e:
            logger.error(f"Error crawling {url}: {e}")
            visited.add(url)

    try:
        driver.quit()
    except Exception:
        pass
                    
def find_common_data(data):
    content_map = defaultdict(set)
    for entry in data:
        content_map[entry['Content']].add(entry['URL'])
    common_data = []
    for content_text, urls in content_map.items():
        if len(urls) > 1:
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
            for url, entries in grouped.items():
                df = pd.DataFrame(entries)
                total_words = df['Word Count'].sum()
                sheet_name = url.replace('http://', '').replace('https://', '').replace('/', '_')[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                worksheet = writer.sheets[sheet_name]
                worksheet.write(len(df) + 1, 0, 'Total Words')
                worksheet.write(len(df) + 1, 4, total_words)
                summary.append({'URL': url, 'Total Words': total_words})

            # Write common data sheet
            df_common = pd.DataFrame(common_data)
            df_common.to_excel(writer, sheet_name='Common Data', index=False)

            # Write summary sheet
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
            for url, entries in grouped.items():
                df = pd.DataFrame(entries)
                # Calculate total words for this URL
                total_words = df['Word Count'].sum()
                # Write data to sheet named by URL (sanitized)
                sheet_name = url.replace('http://', '').replace('https://', '').replace('/', '_')[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                # Write total words at bottom of sheet
                worksheet = writer.sheets[sheet_name]
                worksheet.write(len(df) + 1, 0, 'Total Words')
                worksheet.write(len(df) + 1, 4, total_words)
                # Add to summary
                summary.append({'URL': url, 'Total Words': total_words})

            # Write summary sheet
            df_summary = pd.DataFrame(summary)
            df_summary.to_excel(writer, sheet_name='Summary', index=False)

from django.shortcuts import redirect

def index(request):
    global show_common_data
    if request.method == 'POST':
        url = request.POST.get('url')
        show_common_data = request.POST.get('show_common') == 'on'
        if url:
            thread = threading.Thread(target=crawl_site, args=(url,))
            thread.start()
            thread.join()
            save_to_excel()
            # Redirect to view_data page after scraping completes
            return redirect('view_data')
    return render(request, 'scraper/index.html')

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
