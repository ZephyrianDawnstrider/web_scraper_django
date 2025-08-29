import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin

def is_valid_url(url, base_netloc):
    try:
        parsed = urlparse(url)
        return (parsed.scheme in ("http", "https")) and (parsed.netloc == base_netloc)
    except:
        return False

def debug_sitemap_parsing(start_url):
    base_netloc = urlparse(start_url).netloc
    discovered_urls = set()
    
    # Create a session with retry strategy for requests
    session = requests.Session()
    
    # 1. Check for sitemap.xml
    sitemap_urls = [
        urljoin(start_url, '/sitemap.xml'),
        urljoin(start_url, '/sitemap_index.xml'),
        urljoin(start_url, '/sitemaps.xml'),
        urljoin(start_url, '/sitemap/sitemap.xml')
    ]
    
    print("Checking for sitemaps...")
    sitemap_found = False
    for sitemap_url in sitemap_urls:
        try:
            print(f"Checking sitemap: {sitemap_url}")
            response = session.get(sitemap_url, timeout=10)
            print(f"Response status: {response.status_code}")
            print(f"Content type: {response.headers.get('content-type', '')}")
            if response.status_code == 200 and "xml" in response.headers.get('content-type', ''):
                print(f"Sitemap content preview: {response.text[:500]}")
                soup = BeautifulSoup(response.text, 'xml')
                # Extract URLs from sitemap
                loc_elements = soup.find_all('loc')
                print(f"Found {len(loc_elements)} loc elements in sitemap")
                loc_count = 0
                for loc in loc_elements:
                    url = loc.text.strip()
                    print(f"Found URL in sitemap: {url}")
                    if is_valid_url(url, base_netloc):
                        discovered_urls.add(url)
                        loc_count += 1
                        print(f"Added valid sitemap URL: {url}")
                    else:
                        print(f"URL {url} is not valid for domain {base_netloc}")
                if loc_count > 0:
                    print(f"Found {loc_count} URLs in sitemap {sitemap_url}")
                    sitemap_found = True
                    break
                else:
                    print(f"No valid URLs found in sitemap {sitemap_url}")
            else:
                print(f"Sitemap {sitemap_url} is not valid XML or not found")
        except Exception as e:
            print(f"Sitemap {sitemap_url} not accessible: {e}")
            continue
    
    # 2. Check robots.txt for sitemap references
    try:
        robots_url = urljoin(start_url, '/robots.txt')
        print(f"Checking robots.txt: {robots_url}")
        response = session.get(robots_url, timeout=10)
        print(f"Robots.txt response status: {response.status_code}")
        if response.status_code == 200:
            robots_content = response.text
            print(f"Robots.txt content preview: {robots_content[:500]}")
            import re
            sitemap_matches = re.findall(r'Sitemap:\s*(.+)', robots_content, re.IGNORECASE)
            print(f"Found {len(sitemap_matches)} sitemap references in robots.txt")
            if sitemap_matches:
                for sitemap_url in sitemap_matches:
                    sitemap_url = sitemap_url.strip()
                    try:
                        print(f"Checking robots.txt sitemap: {sitemap_url}")
                        sitemap_response = session.get(sitemap_url, timeout=10)
                        print(f"Robots.txt sitemap response status: {sitemap_response.status_code}")
                        if sitemap_response.status_code == 200 and "xml" in sitemap_response.headers.get('content-type', ''):
                            print(f"Sitemap content preview: {sitemap_response.text[:500]}")
                            soup = BeautifulSoup(sitemap_response.text, 'xml')
                            loc_elements = soup.find_all('loc')
                            print(f"Found {len(loc_elements)} loc elements in sitemap")
                            loc_count = 0
                            for loc in loc_elements:
                                url = loc.text.strip()
                                print(f"Found URL in sitemap: {url}")
                                if is_valid_url(url, base_netloc):
                                    discovered_urls.add(url)
                                    loc_count += 1
                                    print(f"Added valid robots.txt sitemap URL: {url}")
                                else:
                                    print(f"URL {url} is not valid for domain {base_netloc}")
                            print(f"Found {loc_count} valid URLs in robots.txt sitemap {sitemap_url}")
                        else:
                            print(f"Robots.txt sitemap {sitemap_url} is not valid XML or not found")
                    except Exception as e:
                        print(f"Error processing robots.txt sitemap {sitemap_url}: {e}")
            else:
                print("No sitemap references found in robots.txt")
        else:
            print(f"Robots.txt not found or inaccessible")
    except Exception as e:
        print(f"Error checking robots.txt: {e}")
    
    print(f"Total discovered URLs: {len(discovered_urls)}")
    for url in list(discovered_urls)[:10]:  # Show first 10
        print(f"  - {url}")
    
    return discovered_urls

if __name__ == "__main__":
    debug_sitemap_parsing("https://www.chlworldwide.com")
