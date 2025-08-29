import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

def test_sitemap_parsing():
    url = "https://www.chlworldwide.com/sitemap.xml"
    try:
        response = requests.get(url)
        print(f"Status code: {response.status_code}")
        print(f"Content length: {len(response.text)}")
        print("Content preview:")
        print(response.text[:1000])
        
        # Try to parse with BeautifulSoup
        soup = BeautifulSoup(response.text, 'xml')
        loc_elements = soup.find_all('loc')
        print(f"\nFound {len(loc_elements)} loc elements")
        
        base_netloc = urlparse(url).netloc
        valid_urls = []
        
        for loc in loc_elements:
            url_text = loc.text.strip()
            print(f"Found URL: {url_text}")
            
            # Check if valid URL
            try:
                parsed = urlparse(url_text)
                is_valid = (parsed.scheme in ("http", "https")) and (parsed.netloc == base_netloc)
                print(f"  Valid for domain: {is_valid}")
                if is_valid:
                    valid_urls.append(url_text)
            except Exception as e:
                print(f"  Error checking URL: {e}")
        
        print(f"\nTotal valid URLs: {len(valid_urls)}")
        for url in valid_urls[:10]:  # Show first 10
            print(f"  - {url}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_sitemap_parsing()
