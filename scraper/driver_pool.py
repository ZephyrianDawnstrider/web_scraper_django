import threading
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import logging

class WebDriverPool:
    """Thread-safe pool of reusable WebDriver instances"""
    
    def __init__(self, max_drivers=5, headless=True):
        self.max_drivers = max_drivers
        self.headless = headless
        self._drivers = []
        self._lock = threading.Lock()
        self._available = []
        self.logger = logging.getLogger(__name__)
        
    def _create_driver(self):
        """Create a new WebDriver instance with optimized settings"""
        options = Options()
        
        if self.headless:
            options.add_argument('--headless')
        
        # Performance optimizations
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-images')
        options.add_argument('--disable-javascript')
        options.add_argument('--blink-settings=imagesEnabled=false')
        
        # Page load strategy
        caps = DesiredCapabilities().CHROME
        caps["pageLoadStrategy"] = "eager"  # Don't wait for full page load
        
        driver = webdriver.Chrome(options=options, desired_capabilities=caps)
        driver.set_page_load_timeout(30)
        driver.implicitly_wait(5)
        
        return driver
    
    def get_driver(self):
        """Get an available driver from the pool"""
        with self._lock:
            if self._available:
                return self._available.pop()
            
            if len(self._drivers) < self.max_drivers:
                driver = self._create_driver()
                self._drivers.append(driver)
                return driver
            
            # Wait for available driver
            return None
    
    def return_driver(self, driver):
        """Return a driver to the pool"""
        with self._lock:
            if driver in self._drivers:
                self._available.append(driver)
    
    def close_all(self):
        """Close all drivers in the pool"""
        with self._lock:
            for driver in self._drivers:
                try:
                    driver.quit()
                except:
                    pass
            self._drivers.clear()
            self._available.clear()
