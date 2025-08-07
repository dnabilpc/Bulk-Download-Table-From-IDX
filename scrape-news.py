import requests
import pandas as pd
import os
from datetime import datetime, timedelta
import json
import re
import time
from bs4 import BeautifulSoup
import cloudscraper

class StockNewsFilter:
    def __init__(self):
        self.watchlist_file = "watchlist.json"
        self.news_folder = "Stock News"
        os.makedirs(self.news_folder, exist_ok=True)
        
        # Create scraper for web requests
        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        
        # Common headers
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }
    
    def load_watchlist(self):
        """Load stock watchlist from JSON file"""
        if os.path.exists(self.watchlist_file):
            try:
                with open(self.watchlist_file, 'r') as f:
                    return json.load(f)
            except:
                return []
        return []
    
    def save_watchlist(self, watchlist):
        """Save watchlist to JSON file"""
        with open(self.watchlist_file, 'w') as f:
            json.dump(watchlist, f, indent=2)
    
    def manage_watchlist(self):
        """Interactive watchlist management"""
        watchlist = self.load_watchlist()
        
        while True:
            print("\n" + "="*50)
            print("WATCHLIST MANAGEMENT")
            print("="*50)
            
            if watchlist:
                print("Current watchlist:")
                for i, stock in enumerate(watchlist, 1):
                    print(f"  {i}. {stock}")
            else:
                print("Watchlist is empty")
            
            print("\nOptions:")
            print("1. Add stock codes")
            print("2. Remove stock codes") 
            print("3. Clear all")
            print("4. Load from trading signals")
            print("5. Continue to news filter")
            print("6. Exit")
            
            choice = input("\nSelect option (1-6): ").strip()
            
            if choice == '1':
                new_stocks = input("Enter stock codes (comma separated, e.g., BBCA,BMRI,TLKM): ").strip().upper()
                if new_stocks:
                    stocks_to_add = [s.strip() for s in new_stocks.split(',') if s.strip()]
                    for stock in stocks_to_add:
                        if stock not in watchlist:
                            watchlist.append(stock)
                            print(f"✓ Added {stock}")
                        else:
                            print(f"- {stock} already in watchlist")
                    self.save_watchlist(watchlist)
            
            elif choice == '2':
                if not watchlist:
                    print("Watchlist is empty!")
                    continue
                
                stocks_to_remove = input("Enter stock codes to remove (comma separated): ").strip().upper()
                if stocks_to_remove:
                    stocks_list = [s.strip() for s in stocks_to_remove.split(',') if s.strip()]
                    for stock in stocks_list:
                        if stock in watchlist:
                            watchlist.remove(stock)
                            print(f"✓ Removed {stock}")
                        else:
                            print(f"- {stock} not in watchlist")
                    self.save_watchlist(watchlist)
            
            elif choice == '3':
                confirm = input("Clear all stocks from watchlist? (y/N): ").strip().lower()
                if confirm == 'y':
                    watchlist = []
                    self.save_watchlist(watchlist)
                    print("✓ Watchlist cleared")
            
            elif choice == '4':
                watchlist = self.load_from_signals()
                if watchlist:
                    self.save_watchlist(watchlist)
                    print(f"✓ Loaded {len(watchlist)} stocks from trading signals")
            
            elif choice == '5':
                if not watchlist:
                    print("❌ Watchlist is empty! Please add some stocks first.")
                    continue
                return watchlist
            
            elif choice == '6':
                return None
            
            else:
                print("Invalid option!")
    
    def load_from_signals(self):
        """Load BUY signals from trading signals folder"""
        signals_folder = "Trading Signals"
        if not os.path.exists(signals_folder):
            print(f"❌ {signals_folder} folder not found!")
            return []
        
        # Get the most recent signals file
        signal_files = [f for f in os.listdir(signals_folder) if f.startswith('idx_trading_signals_') and f.endswith('.xlsx')]
        if not signal_files:
            print("❌ No trading signals files found!")
            return []
        
        latest_file = sorted(signal_files)[-1]
        filepath = os.path.join(signals_folder, latest_file)
        
        try:
            df = pd.read_excel(filepath)
            # Get stocks with BUY or STRONG BUY signals
            buy_signals = df[df['Signal'].isin(['BUY', 'STRONG BUY'])]
            stock_codes = buy_signals['StockCode'].tolist()
            print(f"Found {len(stock_codes)} stocks with BUY signals in {latest_file}")
            return stock_codes
        except Exception as e:
            print(f"❌ Error loading signals: {e}")
            return []
    
    def scrape_idx_news(self):
        """Scrape news from IDX website"""
        news = []
        try:
            # Try multiple IDX URLs
            urls = [
                "https://www.idx.co.id/en/news/announcement",
                "https://www.idx.co.id/id/berita/berita/",
                "https://www.idx.co.id/"
            ]
            
            for url in urls:
                try:
                    print(f"    Trying: {url}")
                    response = self.scraper.get(url, headers=self.headers, timeout=20)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Look for various news elements
                    selectors = [
                        'article',
                        'div[class*="news"]',
                        'div[class*="berita"]',
                        'a[href*="news"]',
                        'a[href*="berita"]'
                    ]
                    
                    found_items = []
                    for selector in selectors:
                        items = soup.select(selector)
                        found_items.extend(items[:5])
                    
                    for item in found_items[:10]:
                        try:
                            # Try to extract title from various elements
                            title = None
                            link = None
                            
                            if item.name == 'a':
                                title = item.get_text(strip=True)
                                link = item.get('href')
                            else:
                                title_elem = item.find(['h1', 'h2', 'h3', 'h4', 'a'])
                                if title_elem:
                                    title = title_elem.get_text(strip=True)
                                    if title_elem.name == 'a':
                                        link = title_elem.get('href')
                            
                            if title and len(title) > 20:
                                if link and not link.startswith('http'):
                                    link = f"https://www.idx.co.id{link}"
                                
                                news.append({
                                    'title': title,
                                    'date': datetime.now().strftime("%Y-%m-%d"),
                                    'link': link,
                                    'source': 'IDX'
                                })
                        except:
                            continue
                    
                    if news:  # If we found news, stop trying other URLs
                        break
                        
                except Exception as url_error:
                    print(f"    Failed: {url_error}")
                    continue
                    
        except Exception as e:
            print(f"⚠️ Error scraping IDX news: {e}")
        
    def scrape_google_news(self, watchlist):
        """Scrape Google News for specific stock codes"""
        news = []
        try:
            for stock_code in watchlist[:5]:  # Limit to avoid rate limiting
                search_query = f"{stock_code} saham Indonesia"
                url = f"https://news.google.com/search?q={search_query}&hl=id&gl=ID&ceid=ID:id"
                
                print(f"    Searching Google News for: {stock_code}")
                
                try:
                    response = self.scraper.get(url, headers=self.headers, timeout=15)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Look for article links
                    articles = soup.find_all('article')
                    
                    for article in articles[:3]:  # Limit per stock
                        try:
                            title_elem = article.find(['h3', 'h4'])
                            if title_elem:
                                title = title_elem.get_text(strip=True)
                                
                                # Look for the link
                                link_elem = article.find('a', href=True)
                                link = link_elem.get('href') if link_elem else None
                                
                                if title and len(title) > 20:
                                    news.append({
                                        'title': title,
                                        'date': datetime.now().strftime("%Y-%m-%d"),
                                        'link': f"https://news.google.com{link}" if link and not link.startswith('http') else link,
                                        'source': 'Google News',
                                        'matched_stocks': [stock_code]
                                    })
                        except:
                            continue
                    
                    time.sleep(1)  # Rate limiting
                    
                except Exception as stock_error:
                    print(f"    Failed for {stock_code}: {stock_error}")
                    continue
                    
        except Exception as e:
            print(f"⚠️ Error scraping Google News: {e}")
        
        return news
    
    def try_rss_feeds(self):
        """Try to get news from RSS feeds"""
        news = []
        rss_urls = [
            "https://www.idx.co.id/StaticData/NewsAnnouncement/ANNOUNCEMENTSTOCK/rss_announcement.xml",
            "https://investasi.kontan.co.id/rss",
            "https://finance.detik.com/rss"
        ]
        
        for rss_url in rss_urls:
            try:
                print(f"    Trying RSS: {rss_url}")
                response = self.scraper.get(rss_url, headers=self.headers, timeout=15)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'xml')
                    items = soup.find_all('item')
                    
                    for item in items[:5]:
                        try:
                            title = item.find('title').get_text(strip=True) if item.find('title') else 'No title'
                            link = item.find('link').get_text(strip=True) if item.find('link') else None
                            date = item.find('pubDate').get_text(strip=True) if item.find('pubDate') else datetime.now().strftime("%Y-%m-%d")
                            
                            news.append({
                                'title': title,
                                'date': date,
                                'link': link,
                                'source': f'RSS ({rss_url.split("//")[1].split("/")[0]})'
                            })
                        except:
                            continue
            except:
                continue
        
        return news
    
    def scrape_kontan_news(self):
        """Scrape news from Kontan"""
        news = []
        try:
            # Try multiple Kontan URLs
            urls = [
                "https://investasi.kontan.co.id/",
                "https://keuangan.kontan.co.id/",
                "https://www.kontan.co.id/news/investasi",
                "https://www.kontan.co.id/"
            ]
            
            for url in urls:
                try:
                    print(f"    Trying: {url}")
                    response = self.scraper.get(url, headers=self.headers, timeout=20)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Look for news links and titles
                    links = soup.find_all('a', href=True)
                    
                    for link in links[:20]:
                        try:
                            title = link.get_text(strip=True)
                            href = link.get('href')
                            
                            # Filter for relevant titles and links
                            if (title and len(title) > 30 and 
                                any(keyword in title.lower() for keyword in ['saham', 'bursa', 'ihsg', 'investasi', 'emiten']) and
                                href):
                                
                                if not href.startswith('http'):
                                    href = f"https://www.kontan.co.id{href}"
                                
                                news.append({
                                    'title': title,
                                    'date': datetime.now().strftime("%Y-%m-%d"),
                                    'link': href,
                                    'source': 'Kontan'
                                })
                        except:
                            continue
                    
                    if news:  # If we found news, stop trying other URLs
                        break
                        
                except Exception as url_error:
                    print(f"    Failed: {url_error}")
                    continue
                    
        except Exception as e:
            print(f"⚠️ Error scraping Kontan news: {e}")
        
        return news
    
    def scrape_detik_finance(self):
        """Scrape financial news from Detik Finance"""
        news = []
        try:
            # Try multiple Detik Finance URLs
            urls = [
                "https://finance.detik.com/",
                "https://finance.detik.com/bursa-dan-valas",
                "https://finance.detik.com/moneter"
            ]
            
            for url in urls:
                try:
                    print(f"    Trying: {url}")
                    response = self.scraper.get(url, headers=self.headers, timeout=20)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Look for news links
                    links = soup.find_all('a', href=True)
                    
                    for link in links[:20]:
                        try:
                            title = link.get_text(strip=True)
                            href = link.get('href')
                            
                            # Filter for financial news
                            if (title and len(title) > 30 and 
                                any(keyword in title.lower() for keyword in ['saham', 'bursa', 'ihsg', 'emiten', 'investasi', 'rupiah']) and
                                href and 'finance.detik.com' in href):
                                
                                news.append({
                                    'title': title,
                                    'date': datetime.now().strftime("%Y-%m-%d"),
                                    'link': href,
                                    'source': 'Detik Finance'
                                })
                        except:
                            continue
                    
                    if news:  # If we found news, stop trying other URLs
                        break
                        
                except Exception as url_error:
                    print(f"    Failed: {url_error}")
                    continue
                    
        except Exception as e:
            print(f"⚠️ Error scraping Detik Finance: {e}")
        
        return news
    
    def filter_news_by_watchlist(self, news_list, watchlist):
        """Filter news based on stock codes in watchlist"""
        filtered_news = []
        
        for news_item in news_list:
            title = news_item['title'].upper()
            matched_stocks = []
            
            # Check if any watchlist stock is mentioned in the title
            for stock_code in watchlist:
                if stock_code in title:
                    matched_stocks.append(stock_code)
            
            if matched_stocks:
                news_item['matched_stocks'] = matched_stocks
                filtered_news.append(news_item)
        
        return filtered_news
    
    def run_news_filter(self, watchlist):
        """Main function to scrape and filter news"""
        print(f"\n🔍 Searching for news related to {len(watchlist)} stocks in watchlist...")
        print(f"Watchlist: {', '.join(watchlist)}")
        
        all_news = []
        
        # Method 1: Try RSS feeds first (usually more reliable)
        print("\n📡 Trying RSS feeds...")
        rss_news = self.try_rss_feeds()
        if rss_news:
            all_news.extend(rss_news)
            print(f"  Found {len(rss_news)} articles from RSS feeds")
        
        # Method 2: Scrape from multiple sources
        print("\n📰 Scraping news from websites...")
        
        print("  - IDX official website...")
        idx_news = self.scrape_idx_news()
        if idx_news:
            all_news.extend(idx_news)
            print(f"    Found {len(idx_news)} articles")
        
        time.sleep(2)
        
        print("  - Kontan...")
        kontan_news = self.scrape_kontan_news()
        if kontan_news:
            all_news.extend(kontan_news)
            print(f"    Found {len(kontan_news)} articles")
        
        time.sleep(2)
        
        print("  - Detik Finance...")
        detik_news = self.scrape_detik_finance()
        if detik_news:
            all_news.extend(detik_news)
            print(f"    Found {len(detik_news)} articles")
        
        # Method 3: Google News as backup (searches specifically for your stocks)
        if len(all_news) < 5:  # If we don't have much news, try Google
            print("\n🔍 Searching Google News for your specific stocks...")
            google_news = self.scrape_google_news(watchlist)
            if google_news:
                # Google news already has matched_stocks, so add directly to filtered
                all_news.extend(google_news)
                print(f"    Found {len(google_news)} targeted articles")
        
        print(f"\n📊 Total news articles collected: {len(all_news)}")
        
        # Filter news by watchlist (skip if already filtered by Google News)
        google_filtered = [item for item in all_news if item.get('matched_stocks')]
        other_news = [item for item in all_news if not item.get('matched_stocks')]
        
        filtered_news = google_filtered  # Already filtered
        if other_news:
            filtered_from_scraping = self.filter_news_by_watchlist(other_news, watchlist)
            filtered_news.extend(filtered_from_scraping)
        
        if not filtered_news:
            print(f"\n❌ No news found for stocks in your watchlist.")
            print("Possible reasons:")
            print("  - No recent news about your stocks")
            print("  - Stock codes not mentioned in news titles")
            print("  - Websites are blocking scraper access")
            print("  - Try adding more common stocks like BBCA, BMRI, TLKM for testing")
            
            # Show some general news we found
            if all_news:
                print(f"\nℹ️ However, we found {len(all_news)} general financial news articles:")
                for i, news in enumerate(all_news[:5], 1):
                    print(f"  {i}. {news['title'][:80]}...")
            
            return
        
        # Display results
        print(f"\n🎯 Found {len(filtered_news)} news articles matching your watchlist!")
        print("="*100)
        
        for i, news in enumerate(filtered_news, 1):
            print(f"\n📈 News #{i}")
            print(f"Stock(s): {', '.join(news.get('matched_stocks', ['Unknown']))}")
            print(f"Title: {news['title']}")
            print(f"Source: {news['source']}")
            print(f"Date: {news['date']}")
            if news.get('link'):
                print(f"Link: {news['link']}")
            print("-" * 100)
        
        # Save to Excel
        if filtered_news:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"watchlist_news_{timestamp}.xlsx"
            filepath = os.path.join(self.news_folder, filename)
            
            # Flatten matched_stocks for Excel export
            for news in filtered_news:
                if 'matched_stocks' in news:
                    news['matched_stocks_str'] = ', '.join(news['matched_stocks'])
            
            df = pd.DataFrame(filtered_news)
            df.to_excel(filepath, index=False)
            print(f"\n💾 Filtered news saved to: {filepath}")
            
            # Summary by stock
            stock_mentions = {}
            for news in filtered_news:
                for stock in news.get('matched_stocks', []):
                    stock_mentions[stock] = stock_mentions.get(stock, 0) + 1
            
            print(f"\n📈 News mentions per stock:")
            for stock, count in sorted(stock_mentions.items()):
                print(f"  {stock}: {count} mentions")

if __name__ == "__main__":
    print("🗞️ Stock News Filter for Watchlist")
    print("=" * 50)
    
    filter_system = StockNewsFilter()
    
    # Manage watchlist
    watchlist = filter_system.manage_watchlist()
    
    if watchlist:
        filter_system.run_news_filter(watchlist)
        
        # Ask if user wants to run again
        while True:
            run_again = input("\nRun news filter again? (y/n): ").strip().lower()
            if run_again == 'y':
                filter_system.run_news_filter(watchlist)
            else:
                break
        
        print("\n✅ News filtering complete!")
    else:
        print("\n❌ Exiting - no watchlist configured.")