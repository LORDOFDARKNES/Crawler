# Crawler
🧭 Purpose

The crawler is an asynchronous website scanner. It automatically explores a target website, downloads pages and assets, and saves them locally for later analysis or archiving.

It’s useful for:
Mapping all reachable URLs on a domain
Downloading website files (HTML, CSS, JS, images, etc.)
Collecting data for SEO/security/research purposes

⚙️ How It Works

Start URL:
You give it a starting address, e.g. https://example.com.

Async Crawl Engine:
It uses Python’s asyncio and aiohttp to fetch many URLs at once (non-blocking), making it fast and efficient.

Parsing Links:
Each HTML page is parsed with BeautifulSoup to extract new links, images, scripts, and CSS files.
These links are added to the queue if they belong to the same domain.

Download Manager:
Every discovered file (JS, CSS, PNG, etc.) is downloaded and saved under the files/ directory.
HTML pages are recursively explored up to a certain depth.

Persistence (checkpoint):
A small SQLite database (crawler_state.sqlite) stores what was visited and what’s still pending, so you can resume after stopping.
Respect and Safety:
Respects robots.txt rules by default.
Limits request speed (rate limiting) to avoid server overload.
Retries failed downloads automatically.

Results:
All visited URLs are saved in result.txt.
All downloaded content is saved in the files/ folder.
You can safely stop/resume the crawl.

🧩 Technologies Used
asyncio / aiohttp → asynchronous HTTP client
BeautifulSoup → HTML parsing
sqlite3 → lightweight local database for persistence
chardet → auto-detect page encoding
pyfiglet → ASCII art banner for fun
urllib.parse → URL normalization and joining

🛠️ Summary
Think of it as a mini web spider that:
crawls a site intelligently,
downloads everything it finds,
avoids hammering the target server, and
can resume after interruptions.
