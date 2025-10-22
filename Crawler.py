#!/usr/bin/env python3
"""
Strong Async Web Crawler - server-friendly, Windows-safe

Changes in this build to reduce "Connection closed":
 - limit_per_host=2 and PER_HOST_INTERVAL=1.0 (gentler per host)
 - Skip HEAD probes entirely (some servers break on HEAD)
 - Prefer non-stream GET for common static assets (js/css/images/ico/svg)
 - Keep identity encoding + Connection: close
 - Robust retries + fallback remain

Tested on Python 3.10–3.13, Windows & Linux.
"""

import asyncio
import aiohttp
from aiohttp import ClientError, TCPConnector, ClientPayloadError, ClientOSError, ServerDisconnectedError
from bs4 import BeautifulSoup
import urllib.parse
import os
import time
import random
import chardet
import pyfiglet
import sqlite3
import sys
import signal
import platform
from collections import defaultdict
from urllib import robotparser

# -------------------------
# Configuration (tweakable)
# -------------------------
USER_AGENT = "LORDOFDARKNES-Crawler/2.4 (+https://example.com/bot)"

DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "*/*",
    "Accept-Encoding": "identity",   # avoid gzip truncation issues
    "Connection": "close",           # avoid flaky keep-alives
}

CONCURRENCY = 12                 # global tasks
PER_HOST_INTERVAL = 1.0          # gentler: 1s between same-host requests
MAX_RETRIES = 4
RETRY_BACKOFF_BASE = 1.5
MAX_FILE_BYTES = 20 * 1024 * 1024   # 20 MB cap
QUEUE_DB = "crawler_state.sqlite"
FILES_DIR = "files"
RESULT_FILE = "result.txt"
DEFAULT_TIMEOUT = 25
DEFAULT_SCHEME = "https"
ALLOWED_SCHEMES = ("http", "https")
RESPECT_ROBOTS = True
SAME_DOMAIN_ONLY = True
FOLLOW_SUBDOMAINS = False
MAX_DEPTH = 10
MAX_PAGES = 20000
LOG_EVERY = 50
STRIP_QUERY_PARAMS = False
STREAM_RETRIES = 2               # fewer retries; we prefer non-stream for assets
USE_HEAD_PROBE = False           # IMPORTANT: disabled to avoid server issues

# Prefer non-stream GET for these extensions
NONSTREAM_ASSET_EXTS = {
    ".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".ico", ".svg", ".webp"
}

# -------------------------
# Helpers / Persistence
# -------------------------
def print_ascii_banner():
    print(pyfiglet.figlet_format("LORDOFDARKNES Crawler V2", font="standard"))
    print("=" * 70)

def ensure_dirs():
    if not os.path.exists(FILES_DIR):
        os.makedirs(FILES_DIR)

class SQLiteFrontier:
    """SQLite-backed persistence for visited and frontier (resume capability)."""
    def __init__(self, dbpath=QUEUE_DB):
        self.conn = sqlite3.connect(dbpath, check_same_thread=False)
        self._init_tables()

    def _init_tables(self):
        cur = self.conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS visited(url TEXT PRIMARY KEY, ts REAL)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS queue(url TEXT PRIMARY KEY, depth INTEGER, ts REAL)""")
        self.conn.commit()

    def mark_visited(self, url):
        cur = self.conn.cursor()
        cur.execute("INSERT OR REPLACE INTO visited(url, ts) VALUES (?, ?)", (url, time.time()))
        self.conn.commit()

    def is_visited(self, url):
        cur = self.conn.cursor()
        cur.execute("SELECT 1 FROM visited WHERE url = ? LIMIT 1", (url,))
        return cur.fetchone() is not None

    def enqueue(self, url, depth):
        cur = self.conn.cursor()
        cur.execute("INSERT OR REPLACE INTO queue(url, depth, ts) VALUES (?, ?, ?)", (url, depth, time.time()))
        self.conn.commit()

    def dequeue_all(self):
        cur = self.conn.cursor()
        cur.execute("SELECT url, depth FROM queue ORDER BY ts")
        rows = cur.fetchall()
        cur.execute("DELETE FROM queue")
        self.conn.commit()
        return rows

    def clear_queue(self):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM queue")
        self.conn.commit()

    def list_visited(self):
        cur = self.conn.cursor()
        cur.execute("SELECT url FROM visited")
        return [row[0] for row in cur.fetchall()]

    def close(self):
        self.conn.close()

# -------------------------
# Robots & Rate Limiter
# -------------------------
class RobotsManager:
    def __init__(self, session):
        self.parsers = {}  # host -> robotparser.RobotFileParser
        self.session = session

    async def fetch_robots(self, base_url):
        parsed = urllib.parse.urlparse(base_url)
        host = parsed.netloc
        if host in self.parsers:
            return self.parsers[host]
        robots_url = urllib.parse.urlunparse((parsed.scheme, parsed.netloc, "/robots.txt", "", "", ""))
        rp = robotparser.RobotFileParser()
        try:
            async with self.session.get(robots_url, timeout=10, ssl=False) as r:
                if r.status == 200:
                    txt = await r.text()
                    rp.parse(txt.splitlines())
                else:
                    rp = None
        except Exception:
            rp = None
        self.parsers[host] = rp
        return rp

    async def allowed(self, url):
        if not RESPECT_ROBOTS:
            return True
        rp = await self.fetch_robots(url)
        if rp is None:
            return True
        return rp.can_fetch(USER_AGENT, url)

class HostRateLimiter:
    """Per-host rate limiter using last-request timestamps."""
    def __init__(self, min_interval=PER_HOST_INTERVAL):
        self.min_interval = min_interval
        self.last_access = defaultdict(lambda: 0.0)
        self.locks = defaultdict(asyncio.Lock)

    async def wait_for_slot(self, host):
        async with self.locks[host]:
            elapsed = time.time() - self.last_access[host]
            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)
            self.last_access[host] = time.time()

# -------------------------
# Crawler
# -------------------------
class AsyncWebCrawler:
    def __init__(self, start_url, concurrency=CONCURRENCY):
        self.start_url = self._normalize(start_url)
        self.parsed_start = urllib.parse.urlparse(self.start_url)
        self.start_domain = self.parsed_start.netloc
        self.seen = set()
        self.session = None
        self.frontier = asyncio.Queue()
        self.concurrency = concurrency
        self.frontier_db = SQLiteFrontier(QUEUE_DB)
        self.host_limiter = HostRateLimiter()
        self.robots = None
        self.stats = {"downloaded_bytes": 0, "pages": 0}
        self.shutdown = False
        ensure_dirs()

    # ---------- normalization / domain checks ----------
    def _normalize(self, url):
        if not url:
            return url
        parsed = urllib.parse.urlparse(url, allow_fragments=True)
        if not parsed.scheme:
            url = DEFAULT_SCHEME + "://" + url
            parsed = urllib.parse.urlparse(url)
        normalized = urllib.parse.urlunparse((
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path or "/",
            "",
            "" if STRIP_QUERY_PARAMS else parsed.query,
            ""
        ))
        return normalized.rstrip('/')

    def _same_domain(self, url):
        p = urllib.parse.urlparse(url)
        if SAME_DOMAIN_ONLY:
            if FOLLOW_SUBDOMAINS:
                return p.netloc == self.start_domain or p.netloc.endswith("." + self.start_domain)
            else:
                return p.netloc == self.start_domain
        return True

    # ---------- platform-safe signal hooks ----------
    def _install_signal_handlers(self):
        try:
            loop = asyncio.get_running_loop()
            for sig_name in ("SIGINT", "SIGTERM"):
                sig = getattr(signal, sig_name, None)
                if sig is None:
                    continue
                try:
                    loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._request_shutdown()))
                except NotImplementedError:
                    pass  # Windows: rely on KeyboardInterrupt
        except RuntimeError:
            pass

    async def _request_shutdown(self):
        if not self.shutdown:
            print("\n[!] Shutdown requested. Finishing current tasks...")
            self.shutdown = True

    # ---------- main run ----------
    async def start(self):
        # Gentler to servers: limit_per_host=2, force_close sockets, ssl=False for http
        connector = TCPConnector(limit=0, limit_per_host=2, ssl=False, force_close=True)
        async with aiohttp.ClientSession(
            connector=connector,
            headers=DEFAULT_HEADERS,
            trust_env=True
        ) as session:
            self.session = session
            self.robots = RobotsManager(self.session)

            queued = self.frontier_db.dequeue_all()
            if queued:
                for url, depth in queued:
                    await self.frontier.put((url, depth))
                    self.seen.add(url)
            else:
                await self.frontier.put((self.start_url, 0))
                self.seen.add(self.start_url)

            self._install_signal_handlers()

            workers = [asyncio.create_task(self.worker(i)) for i in range(self.concurrency)]
            try:
                await asyncio.gather(*workers)
            finally:
                await self._persist_frontier()

    async def _persist_frontier(self):
        remaining = []
        while not self.frontier.empty():
            try:
                item = self.frontier.get_nowait()
                remaining.append(item)
            except asyncio.QueueEmpty:
                break
        self.frontier_db.clear_queue()
        for url, depth in remaining:
            self.frontier_db.enqueue(url, depth)
        if remaining:
            print(f"[persist] Saved {len(remaining)} queue items to {QUEUE_DB}")

    # ---------- worker & fetch ----------
    async def worker(self, wid):
        while not self.shutdown:
            try:
                url, depth = await asyncio.wait_for(self.frontier.get(), timeout=2.0)
            except asyncio.TimeoutError:
                if self.frontier.empty():
                    break
                continue

            if self.frontier_db.is_visited(url):
                self.frontier.task_done()
                continue

            # robots check
            if RESPECT_ROBOTS and not await self.robots.allowed(url):
                print(f"[robots] Disallowed: {url}")
                self.frontier_db.mark_visited(url)
                self.frontier.task_done()
                continue

            await self.host_limiter.wait_for_slot(urllib.parse.urlparse(url).netloc)

            try:
                await self.crawl_page(url, depth)
            except Exception as e:
                print(f"[worker {wid}] Error crawling {url}: {e}")
            finally:
                self.frontier_db.mark_visited(url)
                self.frontier.task_done()

            if self.stats["pages"] >= MAX_PAGES:
                print(f"[!] Reached MAX_PAGES ({MAX_PAGES}). Stopping.")
                self.shutdown = True
                break

    async def fetch(self, url, method="GET", stream=False, allow_redirects=True):
        """Fetch with retries. Returns (status, headers, data) or (resp, content_type) if stream=True."""
        attempt = 0
        while attempt <= MAX_RETRIES and not self.shutdown:
            try:
                timeout = aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT, sock_read=DEFAULT_TIMEOUT)
                async with self.session.request(method, url, timeout=timeout, allow_redirects=allow_redirects) as resp:
                    if stream:
                        return resp, resp.headers.get("Content-Type", "")
                    data = await resp.read()
                    return resp.status, resp.headers, data
            except (ClientPayloadError, ServerDisconnectedError, ClientError, asyncio.TimeoutError, ClientOSError) as e:
                wait = (RETRY_BACKOFF_BASE ** attempt) + random.random()
                print(f"[retry] {method} {url} failed ({type(e).__name__}: {e}). attempt={attempt} backoff={wait:.1f}s")
                await asyncio.sleep(wait)
                attempt += 1
        return None, None, None

    async def crawl_page(self, url, depth):
        if depth > MAX_DEPTH:
            print(f"[depth] Max depth reached for {url}")
            return
        print(f"[crawl] depth={depth} url={url}")

        # Single GET path (HEAD disabled for flaky servers)
        status, headers, body = await self.fetch(url, method="GET")
        if status is None:
            print(f"[fail] Could not fetch {url}")
            return

        content_type = (headers.get("Content-Type", "") or "").lower()

        if "text/html" in content_type or content_type.startswith("application/xhtml"):
            await self._parse_html_and_enqueue(url, body, depth)
            self.stats["pages"] += 1
            self._maybe_log_stats()
            return

        # Non-HTML: decide non-stream vs stream strategy
        path = urllib.parse.urlparse(url).path
        ext = os.path.splitext(path)[1].lower()

        if ext in NONSTREAM_ASSET_EXTS:
            # Prefer non-stream write for small static assets
            await self._save_nonstream(url, body, ext)
        else:
            # Large/unknown: re-fetch with streaming to cap size
            await self._download_stream(url)

        self._maybe_log_stats()

    def _maybe_log_stats(self):
        if self.stats["pages"] and self.stats["pages"] % LOG_EVERY == 0:
            print(f"[stats] pages={self.stats['pages']} downloaded_bytes={self.stats['downloaded_bytes']}")

    async def _parse_html_and_enqueue(self, base_url, raw_bytes, depth):
        detected = chardet.detect(raw_bytes)
        encoding = detected.get("encoding") or "utf-8"
        try:
            html = raw_bytes.decode(encoding, errors="ignore")
        except Exception:
            html = raw_bytes.decode("utf-8", errors="ignore")
        soup = BeautifulSoup(html, "lxml")

        links = set()
        for tag, attr in (("a", "href"), ("img", "src"), ("script", "src"), ("link", "href")):
            for el in soup.find_all(tag):
                if not el.has_attr(attr):
                    continue
                raw = el.get(attr)
                if not raw:
                    continue
                joined = urllib.parse.urljoin(base_url, raw)
                normalized = self._normalize(joined)
                links.add(normalized)

        import re
        for found in re.findall(r'''["'](https?://[^"'>\s]+)["']''', html):
            links.add(self._normalize(found))

        for link in links:
            if not link or link == base_url:
                continue
            if urllib.parse.urlparse(link).scheme not in ALLOWED_SCHEMES:
                continue
            if not self._same_domain(link):
                continue
            if link in self.seen or self.frontier_db.is_visited(link):
                continue

            self.seen.add(link)
            fname = urllib.parse.urlparse(link).path.split('/')[-1]
            next_depth = depth + 1
            if '.' in fname and not fname.endswith('/'):
                await self.frontier.put((link, next_depth))
                self.frontier_db.enqueue(link, next_depth)
            else:
                if next_depth <= MAX_DEPTH:
                    await self.frontier.put((link, next_depth))
                    self.frontier_db.enqueue(link, next_depth)

    async def _save_nonstream(self, url, body, ext):
        # Protect against unexpectedly huge bodies
        if len(body) > MAX_FILE_BYTES:
            print(f"[nonstream-skip] {url} size {len(body)} > MAX_FILE_BYTES={MAX_FILE_BYTES}")
            return
        fname = os.path.basename(urllib.parse.urlparse(url).path) or f"file{ext or ''}"
        if len(fname) > 120:
            fname = fname[-120:]
        save_path = os.path.join(FILES_DIR, fname)
        base, ext2 = os.path.splitext(save_path)
        counter = 1
        while os.path.exists(save_path):
            save_path = f"{base}_{counter}{ext2}"
            counter += 1
        with open(save_path, "wb") as f:
            f.write(body)
        self.stats["downloaded_bytes"] += len(body)
        print(f"[saved] {url} -> {save_path} ({len(body)} bytes)")

    async def _download_stream(self, url):
        """Streaming with limited retries; falls back to non-stream GET on last resort."""
        print(f"[download-stream] {url}")

        # Resolve a unique filename
        fname = os.path.basename(urllib.parse.urlparse(url).path) or "file"
        if len(fname) > 120:
            fname = fname[-120:]
        save_path = os.path.join(FILES_DIR, fname)
        base, ext = os.path.splitext(save_path)
        counter = 1
        while os.path.exists(save_path):
            save_path = f"{base}_{counter}{ext}"
            counter += 1

        for attempt in range(STREAM_RETRIES):
            try:
                resp, _ctype = await self.fetch(url, method="GET", stream=True)
                if resp is None:
                    raise RuntimeError("No response")

                total = 0
                with open(save_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(32 * 1024):
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > MAX_FILE_BYTES:
                            print(f"[download] Max size for {url} at {total} bytes. Stopping.")
                            break
                        f.write(chunk)

                self.stats["downloaded_bytes"] += total
                print(f"[saved] {url} -> {save_path} ({total} bytes)")
                return
            except (ClientPayloadError, ServerDisconnectedError, ClientOSError) as e:
                backoff = (RETRY_BACKOFF_BASE ** attempt) + random.random()
                print(f"[stream-retry] {url} attempt={attempt+1}/{STREAM_RETRIES} {type(e).__name__}: {e}. Backoff {backoff:.1f}s")
                await asyncio.sleep(backoff)
            except Exception as e:
                backoff = (RETRY_BACKOFF_BASE ** attempt) + random.random()
                print(f"[stream-retry] {url} attempt={attempt+1}/{STREAM_RETRIES} unexpected: {e}. Backoff {backoff:.1f}s")
                await asyncio.sleep(backoff)

        # Final fallback: non-stream GET
        print(f"[fallback] Non-stream GET for {url}")
        status, hdrs, body = await self.fetch(url, method="GET", stream=False)
        if status is None or body is None:
            print(f"[fallback-fail] Could not download {url}")
            return
        if len(body) > MAX_FILE_BYTES:
            print(f"[fallback-skip] Body size {len(body)} > MAX_FILE_BYTES={MAX_FILE_BYTES}")
            return
        with open(save_path, "wb") as f:
            f.write(body)
        self.stats["downloaded_bytes"] += len(body)
        print(f"[saved-fallback] {url} -> {save_path} ({len(body)} bytes)")

    # ---------- results ----------
    def save_results(self):
        print("[saving] Writing visited URLs...")
        try:
            visited = self.frontier_db.list_visited()
            with open(RESULT_FILE, "w", encoding="utf-8") as f:
                for u in visited:
                    f.write(u + "\n")
            print(f"[saved] {len(visited)} urls -> {RESULT_FILE}")
        except Exception as e:
            print(f"[saving] Error: {e}")

    def close(self):
        self.frontier_db.close()

# -------------------------
# Run CLI
# -------------------------
def main():
    print_ascii_banner()
    if len(sys.argv) >= 2:
        target = sys.argv[1]
    else:
        target = input("Enter website URL to crawl (e.g., https://example.com): ").strip()
    if not target.startswith(("http://", "https://")):
        target = DEFAULT_SCHEME + "://" + target

    try:
        concurrency = int(input(f"Concurrent connections (default {CONCURRENCY}): ") or CONCURRENCY)
    except Exception:
        concurrency = CONCURRENCY

    crawler = AsyncWebCrawler(target, concurrency=concurrency)
    start = time.time()
    try:
        asyncio.run(crawler.start())
    except KeyboardInterrupt:
        print("\n[!] KeyboardInterrupt received. Graceful shutdown...")
    finally:
        crawler.save_results()
        crawler.close()
        print("Total time: {:.2f}s".format(time.time() - start))
        print(f"Platform: {platform.system()} | Python: {sys.version.split()[0]}")

if __name__ == "__main__":
    main()
