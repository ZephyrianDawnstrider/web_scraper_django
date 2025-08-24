import sqlite3
import json
import hashlib
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Handles all database operations for scraped data storage"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or 'scraped_data.db'
        self.init_database()
    
    def init_database(self):
        """Initialize the database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create scraped_urls table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraped_urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                title TEXT,
                content TEXT,
                word_count INTEGER DEFAULT 0,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status_code INTEGER,
                content_hash TEXT,
                metadata TEXT
            )
        ''')
        
        # Create url_queue table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS url_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                status TEXT DEFAULT 'pending',
                priority INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP,
                error_message TEXT
            )
        ''')
        
        # Create scraping_sessions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraping_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_name TEXT,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                total_urls INTEGER DEFAULT 0,
                successful_urls INTEGER DEFAULT 0,
                failed_urls INTEGER DEFAULT 0,
                settings TEXT
            )
        ''')
        
        # Create indexes for better performance
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_url_hash ON scraped_urls(content_hash)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_url_status ON url_queue(status)
        ''')
        
        conn.commit()
        conn.close()
    
    def add_url_to_queue(self, url: str, priority: int = 1) -> bool:
        """Add URL to processing queue"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR IGNORE INTO url_queue (url, priority)
                VALUES (?, ?)
            ''', (url, priority))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error adding URL to queue: {e}")
            return False
    
    def store_scraped_data(self, url: str, title: str, content: str, 
                          status_code: int = 200, metadata: Dict = None) -> bool:
        """Store scraped data in database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Generate content hash for deduplication
            content_hash = hashlib.md5(content.encode()).hexdigest()
            word_count = len(content.split()) if content else 0
            
            cursor.execute('''
                INSERT OR REPLACE INTO scraped_urls 
                (url, title, content, word_count, status_code, content_hash, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                url, title, content, word_count, status_code, 
                content_hash, json.dumps(metadata or {})
            ))
            
            # Update queue status
            cursor.execute('''
                UPDATE url_queue 
                SET status = 'completed', processed_at = CURRENT_TIMESTAMP
                WHERE url = ?
            ''', (url,))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Error storing scraped data: {e}")
            return False
    
    def mark_url_failed(self, url: str, error_message: str):
        """Mark URL as failed in queue"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE url_queue 
                SET status = 'failed', error_message = ?, processed_at = CURRENT_TIMESTAMP
                WHERE url = ?
            ''', (error_message, url))
            
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error marking URL as failed: {e}")
    
    def get_pending_urls(self, limit: int = 100) -> List[str]:
        """Get list of pending URLs from queue"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT url FROM url_queue 
                WHERE status = 'pending' 
                ORDER BY priority DESC, created_at ASC 
                LIMIT ?
            ''', (limit,))
            
            urls = [row[0] for row in cursor.fetchall()]
            conn.close()
            return urls
        except Exception as e:
            logger.error(f"Error getting pending URLs: {e}")
            return []
    
    def get_scraped_data(self, url: str = None, limit: int = None) -> List[Dict]:
        """Retrieve scraped data from database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if url:
                cursor.execute('''
                    SELECT * FROM scraped_urls WHERE url = ?
                ''', (url,))
            else:
                query = 'SELECT * FROM scraped_urls ORDER BY scraped_at DESC'
                if limit:
                    query += f' LIMIT {limit}'
                cursor.execute(query)
            
            columns = [description[0] for description in cursor.description]
            results = []
            
            for row in cursor.fetchall():
                result = dict(zip(columns, row))
                if result['metadata']:
                    result['metadata'] = json.loads(result['metadata'])
                results.append(result)
            
            conn.close()
            return results
        except Exception as e:
            logger.error(f"Error retrieving scraped data: {e}")
            return []
    
    def get_scraping_stats(self) -> Dict[str, int]:
        """Get scraping statistics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get total scraped URLs
            cursor.execute('SELECT COUNT(*) FROM scraped_urls')
            total_scraped = cursor.fetchone()[0]
            
            # Get queue statistics
            cursor.execute('''
                SELECT status, COUNT(*) 
                FROM url_queue 
                GROUP BY status
            ''')
            
            queue_stats = dict(cursor.fetchall())
            conn.close()
            
            return {
                'total_scraped': total_scraped,
                'pending': queue_stats.get('pending', 0),
                'completed': queue_stats.get('completed', 0),
                'failed': queue_stats.get('failed', 0)
            }
        except Exception as e:
            logger.error(f"Error getting scraping stats: {e}")
            return {}
    
    def clear_database(self):
        """Clear all data from database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('DELETE FROM scraped_urls')
            cursor.execute('DELETE FROM url_queue')
            cursor.execute('DELETE FROM scraping_sessions')
            
            conn.commit()
            conn.close()
            logger.info("Database cleared successfully")
        except Exception as e:
            logger.error(f"Error clearing database: {e}")
    
    def export_to_excel(self, filename: str = None) -> str:
        """Export scraped data to Excel file"""
        try:
            data = self.get_scraped_data()
            if not data:
                return None
            
            df = pd.DataFrame(data)
            filename = filename or f'scraped_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
            df.to_excel(filename, index=False)
            
            return filename
        except Exception as e:
            logger.error(f"Error exporting to Excel: {e}")
            return None

# Global database manager instance
db_manager = DatabaseManager()
