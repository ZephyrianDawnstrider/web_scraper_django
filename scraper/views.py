import os
import threading
import time
from urllib.parse import urlparse, urljoin
from collections import defaultdict

from django.shortcuts import render, redirect
from django.http import HttpResponse, FileResponse
from django.conf import settings

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd

# Global variables to store scraped data and common data flag
scraped_data = []
show_common_data = False

def create_driver():
    options = Options()
    options.headless = True
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=options)
    return driver

def is_valid_url(url, base_netloc):
    try:
        parsed = urlparse(url)
        return (parsed.scheme in ("http", "https")) and (parsed.netloc == base_netloc)
    except:
        return False

def get_page_name(soup):
    if soup.title:
        return soup.title.string.strip()
    return "No Title"

def extract_content(soup):
    content = []
    for tag in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p']):
        text = tag.get_text(strip=True)
        if text:
            content.append((tag.name, text))
    return content

def crawl_site(start_url, max_pages=50):
    global scraped_data
    scraped_data = []
    visited = set()
    to_visit = [start_url]
    base_netloc = urlparse(start_url).netloc

    driver = create_driver()

    while to_visit and len(visited) < max_pages:
        url = to_visit.pop(0)
        if url in visited:
            continue
        try:
            driver.get(url)
            time.sleep(2)  # wait for dynamic content to load
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            page_name = get_page_name(soup)
            content = extract_content(soup)
            for tag_name, text in content:
                scraped_data.append({
                    'URL': url,
                    'Page Name': page_name,
                    'Heading/Tag': tag_name,
                    'Content': text,
                    'Word Count': len(text.split())
                })
            visited.add(url)
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(url, href)
                if is_valid_url(full_url, base_netloc) and full_url not in visited and full_url not in to_visit:
                    to_visit.append(full_url)
        except Exception as e:
            print(f"Error crawling {url}: {e}")
            visited.add(url)
    driver.quit()

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
