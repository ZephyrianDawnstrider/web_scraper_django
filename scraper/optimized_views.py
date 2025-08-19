from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.shortcuts import render
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from .performance_optimizer import (
    driver_pool, memory_scraper, url_manager, 
    perf_monitor, perf_logger
)
from .models import ScrapedData
import logging

logger = logging.getLogger(__name__)

class OptimizedScraper:
    """Optimized scraper with performance improvements"""
    
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=4)
        
    def scrape_url_optimized(self, url_data):
        """Optimized single URL scraping"""
        url, depth = url_data
        
        try:
            with driver_pool.get_driver() as driver:
                start_time = time.time()
                
                # Fast page load with minimal wait
                driver.get(url)
                
                # Optimized extraction
                title = driver.execute_script(
                    "return document.title || document.querySelector('h1')?.textContent || ''"
                )
                
                description = driver.execute_script(
                    "return document.querySelector('meta[name=\"description\"]')?.content || ''"
                )
                
                links = driver.execute_script(
                    "return Array.from(document.querySelectorAll('a[href]')).slice(0, 10).map(a => a.href)"
                )
                
                # Record performance
                load_time = time.time() - start_time
                perf_monitor.record_url_processed()
                
                return {
                    'url': url,
                    'title': title[:200],  # Limit title length
                    'description': description[:500],  # Limit description
                    'links': links[:5],  # Limit links
                    'load_time': load_time,
                    'depth': depth
                }
                
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return None
    
    def scrape_multiple_urls(self, urls, max_depth=2):
        """Scrape multiple URLs with parallel processing"""
        perf_monitor.start()
        
        # Add initial URLs
        url_manager.add_urls(urls, depth=0)
        
        all_results = []
        processed_count = 0
        
        while processed_count < 100:  # Limit total processing
            batch = []
            for _ in range(10):  # Process in batches of 10
                url_data = url_manager.get_next_url()
                if url_data:
                    batch.append(url_data)
            
            if not batch:
                break
                
            # Process batch
            future_to_url = {
                self.executor.submit(self.scrape_url_optimized, url_data): url_data[0]
                for url_data in batch
            }
            
            for future in as_completed(future_to_url):
                result = future.result()
                if result:
                    all_results.append(result)
                    
                    # Add discovered URLs for next level
                    if result['depth'] < max_depth:
                        url_manager.add_urls(result['links'], depth=result['depth'] + 1)
            
            processed_count += len(batch)
            
            # Memory cleanup
            if processed_count % 50 == 0:
                import gc
                gc.collect()
        
        return all_results
    
    def export_to_excel_optimized(self, data, filename):
        """Export data to Excel with memory optimization"""
        if not data:
            return False
            
        # Convert to DataFrame efficiently
        df = pd.DataFrame(data)
        
        # Optimize data types
        for col in df.select_dtypes(include=['object']):
            if col != 'url':
                df[col] = df[col].astype('category')
        
        # Use optimized Excel writer
        memory_scraper.chunked_excel_export(data, filename, chunk_size=500)
        
        return True
    
    def get_performance_stats(self):
        """Get current performance statistics"""
        stats = perf_monitor.get_stats()
        memory_stats = memory_scraper.get_memory_usage()
        
        return {
            **stats,
            **memory_stats,
            'queue_size': url_manager.size(),
            'active_drivers': driver_pool.created_drivers
        }

# Global optimized scraper instance
optimized_scraper = OptimizedScraper()

@csrf_exempt
def scrape_optimized(request):
    """Optimized scraping endpoint"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            urls = data.get('urls', [])
            max_depth = data.get('max_depth', 2)
            
            if not urls:
                return JsonResponse({'error': 'No URLs provided'}, status=400)
            
            # Start optimized scraping
            results = optimized_scraper.scrape_multiple_urls(urls, max_depth)
            
            # Save to database
            for result in results:
                ScrapedData.objects.create(
                    url=result['url'],
                    title=result['title'],
                    description=result['description'],
                    links=json.dumps(result['links']),
                    load_time=result['load_time']
                )
            
            # Get performance stats
            stats = optimized_scraper.get_performance_stats()
            
            return JsonResponse({
                'success': True,
                'results_count': len(results),
                'performance_stats': stats,
                'sample_results': results[:5]  # Return first 5 results
            })
            
        except Exception as e:
            logger.error(f"Optimized scraping error: {e}")
            return JsonResponse({'error': str(e)}, status=500)
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)

def performance_dashboard(request):
    """Performance monitoring dashboard"""
    stats = optimized_scraper.get_performance_stats()
    
    return render(request, 'scraper/performance.html', {
        'stats': stats,
        'driver_pool_size': driver_pool.max_drivers,
        'memory_usage': stats.get('memory_mb', 0),
        'processing_rate': stats.get('urls_per_second', 0)
    })

def export_excel_optimized(request):
    """Export scraped data to Excel with optimization"""
    try:
        # Get data from database
        data = list(ScrapedData.objects.all().values())
        
        if not data:
            return JsonResponse({'error': 'No data to export'}, status=400)
        
        filename = f"scraped_data_optimized_{int(time.time())}.xlsx"
        filepath = f"media/{filename}"
        
        # Export with optimization
        success = optimized_scraper.export_to_excel_optimized(data, filepath)
        
        if success:
            return JsonResponse({
                'success': True,
                'download_url': f'/media/{filename}',
                'records_exported': len(data)
            })
        
        return JsonResponse({'error': 'Export failed'}, status=500)
        
    except Exception as e:
        logger.error(f"Export error: {e}")
        return JsonResponse({'error': str(e)}, status=500)
