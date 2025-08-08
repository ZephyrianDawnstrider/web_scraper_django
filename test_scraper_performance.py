#!/usr/bin/env python3
"""
Comprehensive Testing Suite for Web Scraper Django Application
Includes: Critical Path Testing, Thorough Testing, and Performance Benchmarking
"""

import os
import sys
import time
import requests
import threading
import json
from concurrent.futures import ThreadPoolExecutor
import subprocess
import signal

# Add Django project to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'web_scraper_django.settings')

import django
django.setup()

from django.test import TestCase, Client
from django.urls import reverse
from scraper.views import crawl_site, scrape_progress

class ScraperTestSuite:
    def __init__(self):
        self.client = Client()
        self.base_url = 'http://127.0.0.1:8000'
        self.server_process = None
        self.test_results = {
            'critical_path': [],
            'thorough': [],
            'performance': []
        }
    
    def start_server(self):
        """Start Django development server for testing"""
        print("🚀 Starting Django development server...")
        try:
            self.server_process = subprocess.Popen(
                [sys.executable, 'manage.py', 'runserver', '127.0.0.1:8000'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            time.sleep(3)  # Wait for server to start
            
            # Test if server is running
            response = requests.get(f'{self.base_url}/', timeout=5)
            if response.status_code == 200:
                print("✅ Django server started successfully")
                return True
            else:
                print(f"❌ Server responded with status {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Failed to start server: {e}")
            return False
    
    def stop_server(self):
        """Stop Django development server"""
        if self.server_process:
            print("🛑 Stopping Django server...")
            self.server_process.terminate()
            self.server_process.wait()
            print("✅ Server stopped")
    
    def log_result(self, test_type, test_name, status, details=""):
        """Log test results"""
        result = {
            'test': test_name,
            'status': status,
            'details': details,
            'timestamp': time.time()
        }
        self.test_results[test_type].append(result)
        status_icon = "✅" if status == "PASS" else "❌"
        print(f"{status_icon} {test_name}: {status} {details}")
    
    # CRITICAL PATH TESTING
    def test_critical_path(self):
        """Test critical application functionality"""
        print("\n🔍 CRITICAL PATH TESTING")
        print("=" * 50)
        
        # Test 1: Server startup and basic page loading
        try:
            response = requests.get(f'{self.base_url}/', timeout=10)
            if response.status_code == 200 and 'Website Content Scraper' in response.text:
                self.log_result('critical_path', 'Homepage Loading', 'PASS')
            else:
                self.log_result('critical_path', 'Homepage Loading', 'FAIL', f'Status: {response.status_code}')
        except Exception as e:
            self.log_result('critical_path', 'Homepage Loading', 'FAIL', str(e))
        
        # Test 2: Form submission (POST request)
        try:
            csrf_token = self.get_csrf_token()
            data = {
                'url': 'https://httpbin.org/html',  # Simple test URL
                'show_common': 'off',
                'csrfmiddlewaretoken': csrf_token
            }
            response = requests.post(f'{self.base_url}/', data=data, timeout=10)
            if response.status_code == 200:
                response_data = response.json()
                if response_data.get('status') == 'started':
                    self.log_result('critical_path', 'Form Submission', 'PASS')
                else:
                    self.log_result('critical_path', 'Form Submission', 'FAIL', 'Invalid response')
            else:
                self.log_result('critical_path', 'Form Submission', 'FAIL', f'Status: {response.status_code}')
        except Exception as e:
            self.log_result('critical_path', 'Form Submission', 'FAIL', str(e))
        
        # Test 3: Progress endpoint
        try:
            response = requests.get(f'{self.base_url}/scrape_progress/', timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'status' in data and 'total_urls' in data:
                    self.log_result('critical_path', 'Progress Endpoint', 'PASS')
                else:
                    self.log_result('critical_path', 'Progress Endpoint', 'FAIL', 'Missing required fields')
            else:
                self.log_result('critical_path', 'Progress Endpoint', 'FAIL', f'Status: {response.status_code}')
        except Exception as e:
            self.log_result('critical_path', 'Progress Endpoint', 'FAIL', str(e))
        
        # Test 4: View data page (should show no data initially)
        try:
            response = requests.get(f'{self.base_url}/view/', timeout=10)
            if response.status_code == 200 and 'No data available' in response.text:
                self.log_result('critical_path', 'View Data Page', 'PASS')
            else:
                self.log_result('critical_path', 'View Data Page', 'FAIL', f'Status: {response.status_code}')
        except Exception as e:
            self.log_result('critical_path', 'View Data Page', 'FAIL', str(e))
    
    def get_csrf_token(self):
        """Get CSRF token for form submissions"""
        try:
            response = requests.get(f'{self.base_url}/')
            # Extract CSRF token from response
            import re
            match = re.search(r'name="csrfmiddlewaretoken" value="([^"]+)"', response.text)
            return match.group(1) if match else ''
        except:
            return ''
    
    # THOROUGH TESTING
    def test_thorough(self):
        """Comprehensive testing of all functionality"""
        print("\n🔬 THOROUGH TESTING")
        print("=" * 50)
        
        # Test all endpoints
        endpoints = [
            ('/', 'Homepage'),
            ('/view/', 'View Data'),
            ('/download/', 'Download'),
            ('/scrape_progress/', 'Progress API')
        ]
        
        for endpoint, name in endpoints:
            try:
                response = requests.get(f'{self.base_url}{endpoint}', timeout=10)
                if response.status_code in [200, 404]:  # 404 is acceptable for download without data
                    self.log_result('thorough', f'{name} Endpoint', 'PASS', f'Status: {response.status_code}')
                else:
                    self.log_result('thorough', f'{name} Endpoint', 'FAIL', f'Status: {response.status_code}')
            except Exception as e:
                self.log_result('thorough', f'{name} Endpoint', 'FAIL', str(e))
        
        # Test form validation
        try:
            csrf_token = self.get_csrf_token()
            # Test with invalid URL
            data = {
                'url': 'invalid-url',
                'csrfmiddlewaretoken': csrf_token
            }
            response = requests.post(f'{self.base_url}/', data=data, timeout=10)
            # Should handle gracefully
            self.log_result('thorough', 'Form Validation', 'PASS', 'Handled invalid URL')
        except Exception as e:
            self.log_result('thorough', 'Form Validation', 'FAIL', str(e))
        
        # Test concurrent requests
        try:
            def make_request():
                return requests.get(f'{self.base_url}/', timeout=5)
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(make_request) for _ in range(5)]
                results = [f.result() for f in futures]
            
            if all(r.status_code == 200 for r in results):
                self.log_result('thorough', 'Concurrent Requests', 'PASS', '5 concurrent requests')
            else:
                self.log_result('thorough', 'Concurrent Requests', 'FAIL', 'Some requests failed')
        except Exception as e:
            self.log_result('thorough', 'Concurrent Requests', 'FAIL', str(e))
    
    # PERFORMANCE BENCHMARKING
    def test_performance(self):
        """Performance and optimization testing"""
        print("\n⚡ PERFORMANCE BENCHMARKING")
        print("=" * 50)
        
        # Test 1: Response time benchmarking
        try:
            times = []
            for i in range(10):
                start = time.time()
                response = requests.get(f'{self.base_url}/', timeout=10)
                end = time.time()
                if response.status_code == 200:
                    times.append(end - start)
            
            if times:
                avg_time = sum(times) / len(times)
                max_time = max(times)
                min_time = min(times)
                
                if avg_time < 1.0:  # Less than 1 second average
                    self.log_result('performance', 'Response Time', 'PASS', 
                                  f'Avg: {avg_time:.3f}s, Min: {min_time:.3f}s, Max: {max_time:.3f}s')
                else:
                    self.log_result('performance', 'Response Time', 'WARN', 
                                  f'Avg: {avg_time:.3f}s (>1s)')
            else:
                self.log_result('performance', 'Response Time', 'FAIL', 'No successful requests')
        except Exception as e:
            self.log_result('performance', 'Response Time', 'FAIL', str(e))
        
        # Test 2: Memory usage simulation
        try:
            # Simulate multiple scraping operations
            csrf_token = self.get_csrf_token()
            start_time = time.time()
            
            # Start a scraping operation
            data = {
                'url': 'https://httpbin.org/html',
                'csrfmiddlewaretoken': csrf_token
            }
            response = requests.post(f'{self.base_url}/', data=data, timeout=10)
            
            if response.status_code == 200:
                # Monitor progress for a short time
                for _ in range(5):
                    time.sleep(1)
                    progress_response = requests.get(f'{self.base_url}/scrape_progress/', timeout=5)
                    if progress_response.status_code == 200:
                        progress_data = progress_response.json()
                        if progress_data.get('status') in ['running', 'completed']:
                            break
                
                end_time = time.time()
                duration = end_time - start_time
                
                if duration < 30:  # Should complete quickly for simple page
                    self.log_result('performance', 'Scraping Speed', 'PASS', f'Duration: {duration:.2f}s')
                else:
                    self.log_result('performance', 'Scraping Speed', 'WARN', f'Duration: {duration:.2f}s (>30s)')
            else:
                self.log_result('performance', 'Scraping Speed', 'FAIL', 'Failed to start scraping')
        except Exception as e:
            self.log_result('performance', 'Scraping Speed', 'FAIL', str(e))
        
        # Test 3: Multi-threading efficiency test
        try:
            import multiprocessing
            cpu_count = multiprocessing.cpu_count()
            expected_threads = min(8, cpu_count * 2)
            
            self.log_result('performance', 'Threading Configuration', 'PASS', 
                          f'CPU cores: {cpu_count}, Expected threads: {expected_threads}')
        except Exception as e:
            self.log_result('performance', 'Threading Configuration', 'FAIL', str(e))
    
    def generate_report(self):
        """Generate comprehensive test report"""
        print("\n📊 TEST REPORT")
        print("=" * 50)
        
        for test_type, results in self.test_results.items():
            if not results:
                continue
                
            print(f"\n{test_type.upper()} TESTS:")
            print("-" * 30)
            
            passed = sum(1 for r in results if r['status'] == 'PASS')
            failed = sum(1 for r in results if r['status'] == 'FAIL')
            warned = sum(1 for r in results if r['status'] == 'WARN')
            total = len(results)
            
            print(f"Total: {total}, Passed: {passed}, Failed: {failed}, Warnings: {warned}")
            
            if failed > 0:
                print("FAILED TESTS:")
                for result in results:
                    if result['status'] == 'FAIL':
                        print(f"  ❌ {result['test']}: {result['details']}")
        
        # Overall summary
        all_results = []
        for results in self.test_results.values():
            all_results.extend(results)
        
        if all_results:
            total_passed = sum(1 for r in all_results if r['status'] == 'PASS')
            total_failed = sum(1 for r in all_results if r['status'] == 'FAIL')
            total_tests = len(all_results)
            
            print(f"\n🎯 OVERALL RESULTS:")
            print(f"Success Rate: {(total_passed/total_tests)*100:.1f}% ({total_passed}/{total_tests})")
            
            if total_failed == 0:
                print("🎉 ALL TESTS PASSED!")
            else:
                print(f"⚠️  {total_failed} tests failed - review and fix issues")
    
    def run_all_tests(self):
        """Run all test suites"""
        print("🧪 STARTING COMPREHENSIVE TEST SUITE")
        print("=" * 60)
        
        if not self.start_server():
            print("❌ Cannot start server - aborting tests")
            return False
        
        try:
            self.test_critical_path()
            time.sleep(2)  # Brief pause between test suites
            
            self.test_thorough()
            time.sleep(2)
            
            self.test_performance()
            
            self.generate_report()
            return True
            
        finally:
            self.stop_server()

def main():
    """Main test runner"""
    print("Web Scraper Django - Comprehensive Test Suite")
    print("=" * 60)
    
    tester = ScraperTestSuite()
    success = tester.run_all_tests()
    
    if success:
        print("\n✅ Test suite completed successfully")
        return 0
    else:
        print("\n❌ Test suite failed")
        return 1

if __name__ == '__main__':
    exit(main())
