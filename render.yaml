from __future__ import annotations

#!/usr/bin/env python3
# =============================================================================
# Crypto News Telegram Bot — Owner-Only Monitoring Assistant
# =============================================================================
# Credentials are hardcoded below (personal / private use only).
# Run: /usr/bin/python3 ok.py
# =============================================================================


import asyncio
import contextlib
import json
import logging
import os
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from logging.handlers import RotatingFileHandler
from typing import Optional
from urllib.parse import urljoin, urlparse

import aiohttp
import feedparser
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from bs4 import BeautifulSoup
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

# =============================================================================
# SECTION 1: HARDCODED CONFIGURATION
# =============================================================================

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

_BOT_TOKEN   = "8809855821:AAHaEUQZNbATetvdNSf8WlqPMwZJy7TbO1g"
_OWNER_ID    = 7237337181
_OPENAI_KEY  = None   # Set to "sk-..." if you have an OpenAI key

_POLL_INTERVAL    = 30
_DB_PATH          = os.path.join(_BASE_DIR, "news_bot.db")
_REQUEST_TIMEOUT  = 15
_MAX_RETRIES      = 3
_MAX_CONCURRENT   = 5
_LOG_LEVEL        = "INFO"
_LOG_FILE         = os.path.join(_BASE_DIR, "news_bot.log")
_LOG_MAX_BYTES    = 5 * 1024 * 1024
_LOG_BACKUP_COUNT = 5
_USER_AGENT       = "Mozilla/5.0 (compatible; CryptoNewsBot/1.0)"

# Only articles published within this many hours will be sent.
_MAX_AGE_HOURS = 48


class Config:
    def __init__(self) -> None:
        self.bot_token: str              = _BOT_TOKEN
        self.owner_id: int               = _OWNER_ID
        self.openai_api_key: Optional[str] = _OPENAI_KEY
        self.poll_interval: int          = _POLL_INTERVAL
        self.db_path: str                = _DB_PATH
        self.request_timeout: int        = _REQUEST_TIMEOUT
        self.max_retries: int            = _MAX_RETRIES
        self.max_concurrent_fetches: int = _MAX_CONCURRENT
        self.user_agent: str             = _USER_AGENT
        self.log_level: str              = _LOG_LEVEL
        self.log_file: str               = _LOG_FILE
        self.log_max_bytes: int          = _LOG_MAX_BYTES
        self.log_backup_count: int       = _LOG_BACKUP_COUNT

        if not self.bot_token:
            raise RuntimeError("BOT_TOKEN is empty.")
        if not self.owner_id:
            raise RuntimeError("OWNER_ID is empty.")


# =============================================================================
# SECTION 2: LOGGING
# =============================================================================

def setup_logging(cfg: Config) -> logging.Logger:
    logger = logging.getLogger("news_bot")
    logger.setLevel(cfg.log_level)
    logger.propagate = False
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)
    try:
        rotating = RotatingFileHandler(
            cfg.log_file, maxBytes=cfg.log_max_bytes,
            backupCount=cfg.log_backup_count, encoding="utf-8",
        )
        rotating.setFormatter(fmt)
        logger.addHandler(rotating)
    except OSError:
        logger.warning("Could not open log file; logging to console only.")
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)
    return logger


log = logging.getLogger("news_bot")


# =============================================================================
# SECTION 3: DATA MODELS
# =============================================================================

@dataclass
class FeedItem:
    title: str
    link: str
    published: Optional[datetime]
    source: str


@dataclass
class Article:
    url: str
    title: str
    text: str
    published: Optional[datetime]
    source: str
    image_url: Optional[str] = None
    category: str = "Crypto"
    summary: str = ""
    unique_id: str = field(default="")

    def __post_init__(self) -> None:
        if not self.unique_id:
            self.unique_id = self.url.strip()


@dataclass
class Source:
    name: str
    category: str
    feed_url: str
    fallback_url: Optional[str] = None


# =============================================================================
# SECTION 4: MONITORED SOURCES (priority order: Telegram -> TON -> Crypto...)
# =============================================================================

SOURCES: list[Source] = [
    # --- Telegram (TOP PRIORITY) -------------------------------------------
    Source("Telegram Blog", "Telegram", "https://telegram.org/blog/rss",
           "https://telegram.org/blog"),

    # --- TON Ecosystem ------------------------------------------------------
    Source("TON Foundation Blog", "TON", "https://ton.org/en/blog/rss.xml",
           "https://ton.org/en/blog"),
    Source("CoinTelegraph TON", "TON", "https://cointelegraph.com/rss/tag/ton",
           "https://cointelegraph.com/tags/ton"),

    # --- Crypto News --------------------------------------------------------
    Source("CoinDesk",      "Crypto", "https://www.coindesk.com/arc/outboundfeeds/rss/",
           "https://www.coindesk.com/"),
    Source("CoinTelegraph", "Crypto", "https://cointelegraph.com/rss",
           "https://cointelegraph.com/"),
    Source("Decrypt",       "Crypto", "https://decrypt.co/feed",
           "https://decrypt.co/news"),

    # --- NFT News -----------------------------------------------------------
    Source("CoinTelegraph NFT", "NFT", "https://cointelegraph.com/rss/tag/nft",
           "https://cointelegraph.com/tags/nft"),
    Source("NFT Plazas",    "NFT", "https://nftplazas.com/feed/",
           "https://nftplazas.com/news/"),

    # --- Exchange Announcements --------------------------------------------
    Source("Binance",  "Exchange", "https://www.binance.com/en/blog/rss",
           "https://www.binance.com/en/support/announcement"),
    Source("Coinbase", "Exchange", "https://www.coinbase.com/blog/rss",
           "https://www.coinbase.com/blog"),
    Source("Kraken",   "Exchange", "https://blog.kraken.com/feed",
           "https://blog.kraken.com/"),
    Source("Bybit",    "Exchange", "https://announcements.bybit.com/en-US/rss/",
           "https://announcements.bybit.com/en-US/"),
    Source("OKX",      "Exchange", "https://www.okx.com/help/rss/announcements",
           "https://www.okx.com/help/section/announcements"),
    Source("Bitget",   "Exchange", "https://www.bitget.com/support/rss",
           "https://www.bitget.com/support/sections/12508313443481"),
    Source("KuCoin",   "Exchange", "https://www.kucoin.com/rss/news",
           "https://www.kucoin.com/announcement"),

    # --- Security Alerts ---------------------------------------------------
    Source("CoinTelegraph Hacks", "Security",
           "https://cointelegraph.com/rss/tag/hacks",
           "https://cointelegraph.com/tags/hacks"),
    Source("The Hacker News", "Security",
           "https://feeds.feedburner.com/TheHackersNews",
           "https://thehackernews.com/"),

    # --- Scam Alerts -------------------------------------------------------
    Source("CoinTelegraph Scams", "Scam",
           "https://cointelegraph.com/rss/tag/scams",
           "https://cointelegraph.com/tags/scams"),
]

CATEGORY_PRIORITY: dict[str, int] = {
    "Telegram": 1,
    "TON":      2,
    "Crypto":   3,
    "NFT":      4,
    "Exchange": 5,
    "Security": 6,
    "Scam":     7,
}

CATEGORIES = sorted(CATEGORY_PRIORITY, key=lambda c: CATEGORY_PRIORITY[c])

CATEGORY_HASHTAGS: dict[str, str] = {
    "Telegram": "#Telegram #TelegramUpdate #News",
    "TON":      "#TON #TonCoin #TONBlockchain #CryptoNews",
    "Crypto":   "#Crypto #CryptoNews #Bitcoin #Blockchain",
    "NFT":      "#NFT #NFTNews #Web3",
    "Exchange": "#Exchange #CryptoExchange #Trading",
    "Security": "#Security #CryptoHack #Alert #CyberSecurity",
    "Scam":     "#Scam #CryptoScam #Alert #StaySafe",
}

CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "Scam":     ["scam", "rug pull", "rug-pull", "phishing", "ponzi", "fraudulent",
                 "fraud alert", "exit scam", "impersonat", "fake giveaway"],
    "Security": ["hack", "hacked", "hacker", "exploit", "vulnerability", "breach",
                 "stolen", "security incident", "compromised", "malware", "drained"],
    "Exchange": ["binance", "bybit", "okx", "coinbase", "kraken", "bitget", "kucoin",
                 "exchange listing", "delisting", "trading pair"],
    "TON":      ["ton blockchain", "the open network", "toncoin", "ton ecosystem", " ton "],
    "Telegram": ["telegram app", "telegram update", "telegram messenger",
                 "telegram version", "telegram premium", "telegram feature"],
    "NFT":      ["nft", "non-fungible token", "opensea", "digital collectible", "mint pass"],
    "Crypto":   ["crypto", "bitcoin", "ethereum", "blockchain", "altcoin", "token",
                 "defi", "web3"],
}


def categorize_article(source_category: str, title: str, text: str) -> str:
    combined = f" {title} {text} ".lower()
    for category in ("Scam", "Security", "Exchange", "TON", "Telegram", "NFT", "Crypto"):
        for keyword in CATEGORY_KEYWORDS[category]:
            if keyword in combined:
                return category
    return source_category


def is_fresh(published: Optional[datetime]) -> bool:
    if published is None:
        return True  # unknown date = allow through
    cutoff = datetime.now(timezone.utc) - timedelta(hours=_MAX_AGE_HOURS)
    if published.tzinfo is None:
        published = published.replace(tzinfo=timezone.utc)
    return published >= cutoff


# =============================================================================
# SECTION 5: DATABASE
# =============================================================================

class Database:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._init_sync()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_sync(self) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS sent_articles (
                    unique_id TEXT PRIMARY KEY,
                    url       TEXT NOT NULL,
                    title     TEXT,
                    category  TEXT,
                    sent_at   TEXT NOT NULL
                )""")
            conn.execute(
                """CREATE TABLE IF NOT EXISTS stats (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )""")
            conn.commit()
        finally:
            conn.close()

    def _is_sent_sync(self, uid: str) -> bool:
        conn = self._connect()
        try:
            cur = conn.execute(
                "SELECT 1 FROM sent_articles WHERE unique_id = ? LIMIT 1", (uid,))
            return cur.fetchone() is not None
        finally:
            conn.close()

    def _mark_sent_sync(self, article: Article) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """INSERT OR IGNORE INTO sent_articles
                       (unique_id, url, title, category, sent_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (article.unique_id, article.url, article.title,
                 article.category, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
        finally:
            conn.close()

    async def is_sent(self, uid: str) -> bool:
        return await asyncio.to_thread(self._is_sent_sync, uid)

    async def mark_sent(self, article: Article) -> None:
        await asyncio.to_thread(self._mark_sent_sync, article)

    def _load_stats_sync(self) -> dict:
        conn = self._connect()
        try:
            cur = conn.execute("SELECT key, value FROM stats")
            return {row[0]: row[1] for row in cur.fetchall()}
        finally:
            conn.close()

    def _save_stats_sync(self, data: dict) -> None:
        conn = self._connect()
        try:
            for key, value in data.items():
                conn.execute(
                    "INSERT INTO stats (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (key, json.dumps(value)),
                )
            conn.commit()
        finally:
            conn.close()

    async def load_stats(self) -> dict:
        raw = await asyncio.to_thread(self._load_stats_sync)
        return {k: json.loads(v) for k, v in raw.items()}

    async def save_stats(self, data: dict) -> None:
        await asyncio.to_thread(self._save_stats_sync, data)


# =============================================================================
# SECTION 6: HTTP FETCHER
# =============================================================================

class HttpFetcher:
    def __init__(self, session: aiohttp.ClientSession, cfg: Config) -> None:
        self.session   = session
        self.cfg       = cfg
        self.semaphore = asyncio.Semaphore(cfg.max_concurrent_fetches)

    async def fetch_text(self, url: str) -> Optional[str]:
        headers = {"User-Agent": self.cfg.user_agent, "Accept": "*/*"}
        timeout = aiohttp.ClientTimeout(total=self.cfg.request_timeout)
        last_error: Optional[BaseException] = None
        async with self.semaphore:
            for attempt in range(1, self.cfg.max_retries + 1):
                try:
                    async with self.session.get(
                        url, headers=headers, timeout=timeout, ssl=False
                    ) as resp:
                        if resp.status >= 400:
                            last_error = RuntimeError(f"HTTP {resp.status}")
                            log.warning("HTTP %s for %s (attempt %d/%d)",
                                        resp.status, url, attempt, self.cfg.max_retries)
                        else:
                            raw = await resp.read()
                            try:
                                return raw.decode(resp.charset or "utf-8", errors="replace")
                            except (LookupError, TypeError):
                                return raw.decode("utf-8", errors="replace")
                except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
                    last_error = exc
                    log.warning("Fetch error %s (attempt %d/%d): %s",
                                url, attempt, self.cfg.max_retries, exc)
                if attempt < self.cfg.max_retries:
                    await asyncio.sleep(min(2 ** attempt, 10))
        log.error("Giving up on %s: %s", url, last_error)
        return None


# =============================================================================
# SECTION 7: FEED FETCHING
# =============================================================================

class FeedFetcher:
    _ARTICLE_HREF_HINTS = (
        "/news/", "/blog/", "/article/", "/announcement", "/press",
        "/en/blog", "/tag/", "/posts/", "/updates/",
    )

    def __init__(self, http: HttpFetcher) -> None:
        self.http = http

    async def fetch_source(self, source: Source) -> list[FeedItem]:
        items = await self._fetch_rss(source)
        if items:
            return items
        if source.fallback_url:
            log.info("RSS empty for %s; scraping %s", source.name, source.fallback_url)
            items = await self._fetch_official_page(source)
        return items

    async def _fetch_rss(self, source: Source) -> list[FeedItem]:
        try:
            raw = await self.http.fetch_text(source.feed_url)
            if raw is None:
                return []
            parsed = await asyncio.to_thread(feedparser.parse, raw)
            if parsed.bozo and not parsed.entries:
                return []
            items: list[FeedItem] = []
            for entry in parsed.entries:
                title = getattr(entry, "title", "").strip()
                link  = getattr(entry, "link",  "").strip()
                if not title or not link:
                    continue
                published = self._extract_entry_date(entry)
                items.append(FeedItem(title=title, link=link,
                                      published=published, source=source.name))
            return items
        except Exception as exc:
            log.error("RSS fetch failed for %s: %s", source.name, exc)
            return []

    @staticmethod
    def _extract_entry_date(entry) -> Optional[datetime]:
        for attr in ("published", "updated", "created"):
            raw = getattr(entry, attr, None)
            if not raw:
                continue
            try:
                dt = parsedate_to_datetime(raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except (TypeError, ValueError, IndexError):
                continue
        struct = (getattr(entry, "published_parsed", None)
                  or getattr(entry, "updated_parsed", None))
        if struct:
            try:
                return datetime(*struct[:6], tzinfo=timezone.utc)
            except (TypeError, ValueError):
                return None
        return None

    async def _fetch_official_page(self, source: Source) -> list[FeedItem]:
        try:
            html = await self.http.fetch_text(source.fallback_url)
            if html is None:
                return []
            soup = BeautifulSoup(html, "lxml")
            base = source.fallback_url
            candidates: list[FeedItem] = []
            seen: set[str] = set()
            for anchor in soup.find_all("a", href=True):
                href = anchor["href"].strip()
                text = anchor.get_text(" ", strip=True)
                if not text or len(text) < 20:
                    continue
                if not any(h in href.lower() for h in self._ARTICLE_HREF_HINTS):
                    continue
                absolute = urljoin(base, href)
                if urlparse(absolute).netloc != urlparse(base).netloc:
                    continue
                if absolute in seen:
                    continue
                seen.add(absolute)
                candidates.append(FeedItem(title=text, link=absolute,
                                            published=None, source=source.name))
                if len(candidates) >= 15:
                    break
            return candidates
        except Exception as exc:
            log.error("Official page scrape failed for %s: %s", source.name, exc)
            return []


# =============================================================================
# SECTION 8: ARTICLE EXTRACTION
# =============================================================================

_STRIP_TAGS = ("script", "style", "nav", "header", "footer", "aside",
               "form", "iframe", "noscript", "button", "svg")
_STRIP_HINTS = ("advert", "ads-", "ad-slot", "banner", "cookie", "subscribe",
                "newsletter", "social", "share", "sharing", "related", "sidebar",
                "comment", "popup", "promo", "breadcrumb", "menu", "nav-",
                "site-header", "site-footer", "widget", "recirculation", "paywall")
_CONTENT_SELECTORS = ("article", "div.article-body", "div.post-content",
                       "div.entry-content", "div.content-body",
                       "div.article__content", "div.article-content", "main")


class ArticleExtractor:
    def __init__(self, http: HttpFetcher) -> None:
        self.http = http

    async def extract(self, item: FeedItem) -> Optional[Article]:
        html = await self.http.fetch_text(item.link)
        if html is None:
            return None
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            soup = BeautifulSoup(html, "html.parser")

        title     = self._extract_title(soup, item.title)
        image_url = self._extract_image(soup)
        published = self._extract_date(soup) or item.published
        text      = self._extract_text(soup)

        if not text or len(text.split()) < 30:
            log.warning("Insufficient text from %s; skipping.", item.link)
            return None

        return Article(url=item.link, title=title, text=text, published=published,
                       source=item.source, image_url=image_url)

    @staticmethod
    def _clean_soup(soup: BeautifulSoup) -> None:
        for tag_name in _STRIP_TAGS:
            for tag in soup.find_all(tag_name):
                tag.decompose()
        for tag in soup.find_all(True):
            cls   = tag.get("class") or []
            attrs = (" ".join(cls) + " " + (tag.get("id") or "")).lower()
            if any(hint in attrs for hint in _STRIP_HINTS):
                tag.decompose()

    @staticmethod
    def _extract_title(soup: BeautifulSoup, fallback: str) -> str:
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            return og["content"].strip()
        if soup.title and soup.title.get_text(strip=True):
            return soup.title.get_text(strip=True)
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            return h1.get_text(strip=True)
        return fallback

    @staticmethod
    def _extract_image(soup: BeautifulSoup) -> Optional[str]:
        # Only use explicit article-level meta images — avoids site logos/icons
        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            url = og["content"].strip()
            if url.startswith("http"):
                return url
        tw = soup.find("meta", attrs={"name": "twitter:image"})
        if tw and tw.get("content"):
            url = tw["content"].strip()
            if url.startswith("http"):
                return url
        return None

    @staticmethod
    def _extract_date(soup: BeautifulSoup) -> Optional[datetime]:
        for prop in ("article:published_time", "og:article:published_time"):
            meta = soup.find("meta", property=prop)
            if meta and meta.get("content"):
                with contextlib.suppress(ValueError):
                    return datetime.fromisoformat(
                        meta["content"].replace("Z", "+00:00"))
        time_tag = soup.find("time")
        if time_tag and time_tag.get("datetime"):
            with contextlib.suppress(ValueError):
                return datetime.fromisoformat(
                    time_tag["datetime"].replace("Z", "+00:00"))
        return None

    def _extract_text(self, soup: BeautifulSoup) -> str:
        self._clean_soup(soup)
        content_node = None
        for selector in _CONTENT_SELECTORS:
            found = soup.select_one(selector)
            if found and len(found.get_text(strip=True)) > 200:
                content_node = found
                break
        if content_node is None:
            best_node, best_len = None, 0
            for div in soup.find_all(["div", "section"]):
                paragraphs = div.find_all("p", recursive=False)
                text_len = sum(len(p.get_text(strip=True)) for p in paragraphs)
                if text_len > best_len:
                    best_len = text_len
                    best_node = div
            content_node = best_node or soup.body or soup
        paragraphs = content_node.find_all("p")
        text_parts = [p.get_text(" ", strip=True) for p in paragraphs
                      if len(p.get_text(strip=True).split()) > 3]
        return re.sub(r"\s+", " ", " ".join(text_parts)).strip()


# =============================================================================
# SECTION 9: SUMMARIZATION — 30-70 words
# =============================================================================

_STOPWORDS = frozenset(
    """a about above after again against all am an and any are aren't as at be
    because been before being below between both but by can't cannot could couldn't
    did didn't do does doesn't doing don't down during each few for from further
    had hadn't has hasn't have haven't having he he'd he'll he's her here here's
    hers herself him himself his how how's i i'd i'll i'm i've if in into is isn't
    it it's its itself let's me more most mustn't my myself no nor not of off on
    once only or other ought our ours ourselves out over own same shan't she she'd
    she'll she's should shouldn't so some such than that that's the their theirs
    them themselves then there there's these they they'd they'll they're they've
    this those through to too under until up very was wasn't we we'd we'll we're
    we've were weren't what what's when when's where where's which while who who's
    whom why why's with won't would wouldn't you you'd you'll you're you've your
    yours yourself yourselves said also new says its""".split()
)

_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9\"'])")
_WORD_RE = re.compile(r"[A-Za-z']+")


class ExtractiveSummarizer:
    MIN_WORDS = 30
    MAX_WORDS = 70

    def summarize(self, text: str) -> str:
        sentences = self._split_sentences(text)
        if not sentences:
            return ""
        if len(" ".join(sentences).split()) <= self.MAX_WORDS:
            return self._trim_to_budget(sentences)
        scores = self._score_sentences(sentences)
        ranked = sorted(range(len(sentences)), key=lambda i: scores[i], reverse=True)
        chosen: list[int] = []
        word_count = 0
        for idx in ranked:
            words_here = len(sentences[idx].split())
            if word_count + words_here > self.MAX_WORDS and chosen:
                continue
            chosen.append(idx)
            word_count += words_here
            if word_count >= self.MIN_WORDS:
                break
        chosen.sort()
        return self._trim_to_budget([sentences[i] for i in chosen])

    @staticmethod
    def _split_sentences(text: str) -> list[str]:
        text = text.strip()
        if not text:
            return []
        return [s.strip() for s in _SENTENCE_SPLIT_RE.split(text)
                if len(s.strip().split()) >= 4]

    @staticmethod
    def _score_sentences(sentences: list[str]) -> list[float]:
        freq: dict[str, int] = {}
        for s in sentences:
            for w in _WORD_RE.findall(s.lower()):
                if w in _STOPWORDS or len(w) < 3:
                    continue
                freq[w] = freq.get(w, 0) + 1
        if not freq:
            return [0.0] * len(sentences)
        max_freq   = max(freq.values())
        normalized = {w: c / max_freq for w, c in freq.items()}
        scores: list[float] = []
        for pos, sentence in enumerate(sentences):
            words = [w for w in _WORD_RE.findall(sentence.lower()) if w not in _STOPWORDS]
            if not words:
                scores.append(0.0)
                continue
            raw   = sum(normalized.get(w, 0.0) for w in words) / len(words)
            boost = 1.0 + max(0.0, (5 - pos) * 0.02)
            scores.append(raw * boost)
        return scores

    def _trim_to_budget(self, sentences: list[str]) -> str:
        text  = " ".join(sentences)
        words = text.split()
        if len(words) <= self.MAX_WORDS:
            return text
        trimmed = " ".join(words[: self.MAX_WORDS])
        if not trimmed.endswith((".", "!", "?")):
            trimmed += "."
        return trimmed


class Summarizer:
    def __init__(self, http: HttpFetcher, cfg: Config) -> None:
        self.http       = http
        self.cfg        = cfg
        self.extractive = ExtractiveSummarizer()

    async def summarize(self, title: str, text: str) -> str:
        if self.cfg.openai_api_key:
            ai = await self._openai_summary(title, text)
            if ai:
                return ai
        return self.extractive.summarize(text)

    async def _openai_summary(self, title: str, text: str) -> Optional[str]:
        excerpt = " ".join(text.split()[:800])
        prompt  = (
            "Summarize this crypto/tech news article in professional Telegram style. "
            "Use ONLY facts in the article. "
            "Target: 30 to 70 words, 2-3 concise sentences.\n\n"
            f"Title: {title}\n\nArticle:\n{excerpt}"
        )
        payload = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": "You are a precise crypto news summarizer."},
                {"role": "user",   "content": prompt},
            ],
            "temperature": 0.2,
            "max_tokens": 150,
        }
        headers = {
            "Authorization": f"Bearer {self.cfg.openai_api_key}",
            "Content-Type":  "application/json",
        }
        timeout = aiohttp.ClientTimeout(total=self.cfg.request_timeout)
        try:
            async with self.http.session.post(
                "https://api.openai.com/v1/chat/completions",
                json=payload, headers=headers, timeout=timeout,
            ) as resp:
                if resp.status != 200:
                    log.warning("OpenAI HTTP %s", resp.status)
                    return None
                data    = await resp.json()
                content = data["choices"][0]["message"]["content"].strip()
                wc      = len(content.split())
                if wc < 10:
                    return None
                if wc > 70:
                    content = " ".join(content.split()[:70])
                    if not content.endswith((".", "!", "?")):
                        content += "."
                return content
        except (aiohttp.ClientError, asyncio.TimeoutError, KeyError, json.JSONDecodeError) as exc:
            log.warning("OpenAI request failed: %s", exc)
            return None


# =============================================================================
# SECTION 10: TELEGRAM NOTIFIER
# =============================================================================

class TelegramNotifier:
    CAPTION_LIMIT = 1024

    def __init__(self, application: Application, owner_id: int, cfg: Config) -> None:
        self.application = application
        self.owner_id    = owner_id
        self.cfg         = cfg

    @staticmethod
    def format_message(article: Article) -> str:
        time_str = (
            article.published.astimezone(timezone.utc).strftime("%d %b %Y %H:%M UTC")
            if article.published else "Today"
        )
        hashtags = CATEGORY_HASHTAGS.get(article.category, "#CryptoNews")
        return (
            f"🔔 #{article.category} #News\n\n"
            f"📰 {article.title}\n\n"
            f"📝 {article.summary}\n\n"
            f"🌐 {article.source}  |  🕒 {time_str}\n"
            f"🔗 {article.url}\n\n"
            f"{hashtags}"
        )

    async def send_article(self, article: Article) -> bool:
        message = self.format_message(article)
        bot     = self.application.bot

        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                if article.image_url:
                    if len(message) <= self.CAPTION_LIMIT:
                        await bot.send_photo(chat_id=self.owner_id,
                                             photo=article.image_url, caption=message)
                    else:
                        await bot.send_photo(chat_id=self.owner_id, photo=article.image_url)
                        await bot.send_message(chat_id=self.owner_id, text=message,
                                               disable_web_page_preview=False)
                else:
                    await bot.send_message(chat_id=self.owner_id, text=message,
                                           disable_web_page_preview=False)
                return True
            except TelegramError as exc:
                log.warning("Send failed for %s (attempt %d/%d): %s",
                            article.url, attempt, self.cfg.max_retries, exc)
                if article.image_url and attempt == self.cfg.max_retries - 1:
                    article.image_url = None
                if attempt < self.cfg.max_retries:
                    await asyncio.sleep(min(2 ** attempt, 10))
            except Exception as exc:
                log.error("Unexpected send error: %s", exc)
                break
        return False

    async def send_plain(self, text: str) -> None:
        with contextlib.suppress(TelegramError):
            await self.application.bot.send_message(chat_id=self.owner_id, text=text)


# =============================================================================
# SECTION 11: STATS
# =============================================================================

class Stats:
    def __init__(self, db: Database) -> None:
        self.db                  = db
        self.start_time          = datetime.now(timezone.utc)
        self.total_checked       = 0
        self.total_sent          = 0
        self.duplicates_ignored  = 0
        self.too_old_skipped     = 0
        self.errors              = 0
        self.last_check_time: Optional[datetime] = None
        self.monitoring_enabled  = True

    async def load(self) -> None:
        data = await self.db.load_stats()
        self.total_checked      = int(data.get("total_checked",      0))
        self.total_sent         = int(data.get("total_sent",         0))
        self.duplicates_ignored = int(data.get("duplicates_ignored", 0))
        self.too_old_skipped    = int(data.get("too_old_skipped",    0))
        self.errors             = int(data.get("errors",             0))
        log.info("Loaded persisted stats: %s", self.as_dict())

    async def persist(self) -> None:
        await self.db.save_stats({
            "total_checked":      self.total_checked,
            "total_sent":         self.total_sent,
            "duplicates_ignored": self.duplicates_ignored,
            "too_old_skipped":    self.too_old_skipped,
            "errors":             self.errors,
        })

    def uptime_str(self) -> str:
        delta        = datetime.now(timezone.utc) - self.start_time
        total_secs   = int(delta.total_seconds())
        days, rem    = divmod(total_secs, 86400)
        hours, rem   = divmod(rem, 3600)
        minutes, sec = divmod(rem, 60)
        parts = []
        if days:          parts.append(f"{days}d")
        if hours or days: parts.append(f"{hours}h")
        parts.append(f"{minutes}m")
        parts.append(f"{sec}s")
        return " ".join(parts)

    def as_dict(self) -> dict:
        return {
            "total_checked":      self.total_checked,
            "total_sent":         self.total_sent,
            "duplicates_ignored": self.duplicates_ignored,
            "too_old_skipped":    self.too_old_skipped,
            "errors":             self.errors,
        }


# =============================================================================
# SECTION 12: MONITOR SERVICE (collect -> sort by priority -> send)
# =============================================================================

class MonitorService:
    def __init__(
        self, cfg: Config, db: Database, feed_fetcher: FeedFetcher,
        extractor: ArticleExtractor, summarizer: Summarizer,
        notifier: TelegramNotifier, stats: Stats,
    ) -> None:
        self.cfg          = cfg
        self.db           = db
        self.feed_fetcher = feed_fetcher
        self.extractor    = extractor
        self.summarizer   = summarizer
        self.notifier     = notifier
        self.stats        = stats
        self._lock        = asyncio.Lock()

    async def run_cycle(self) -> tuple[int, int]:
        async with self._lock:
            checked_before = self.stats.total_checked
            sent_before    = self.stats.total_sent

            tasks   = [self._prepare_source(src) for src in SOURCES]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            all_articles: list[Article] = []
            for result in results:
                if isinstance(result, Exception):
                    self.stats.errors += 1
                    log.error("Source exception: %s", result)
                elif isinstance(result, list):
                    all_articles.extend(result)

            # Sort by category priority before sending
            all_articles.sort(key=lambda a: CATEGORY_PRIORITY.get(a.category, 99))

            for article in all_articles:
                await self._deliver(article)

            self.stats.last_check_time = datetime.now(timezone.utc)
            await self.stats.persist()

            return (
                self.stats.total_checked - checked_before,
                self.stats.total_sent    - sent_before,
            )

    async def _prepare_source(self, source: Source) -> list[Article]:
        ready: list[Article] = []
        try:
            items = await self.feed_fetcher.fetch_source(source)
        except Exception as exc:
            self.stats.errors += 1
            log.error("Failed to fetch %s: %s", source.name, exc)
            return ready

        for item in items:
            try:
                article = await self._prepare_item(source, item)
                if article:
                    ready.append(article)
            except Exception as exc:
                self.stats.errors += 1
                log.error("Failed to prepare '%s' from %s: %s", item.title, source.name, exc)
        return ready

    async def _prepare_item(self, source: Source, item: FeedItem) -> Optional[Article]:
        self.stats.total_checked += 1

        # Freshness check (RSS date)
        if not is_fresh(item.published):
            self.stats.too_old_skipped += 1
            return None

        # Deduplication (link level)
        if await self.db.is_sent(item.link):
            self.stats.duplicates_ignored += 1
            return None

        # Full extraction
        article = await self.extractor.extract(item)
        if article is None:
            return None

        article.unique_id = article.url

        # Deduplication (canonical URL)
        if await self.db.is_sent(article.unique_id):
            self.stats.duplicates_ignored += 1
            return None

        # Freshness check (article page date — more accurate)
        if not is_fresh(article.published):
            self.stats.too_old_skipped += 1
            return None

        article.category = categorize_article(source.category, article.title, article.text)
        article.summary  = await self.summarizer.summarize(article.title, article.text)

        if not article.summary:
            log.warning("Empty summary for %s; skipping.", article.url)
            return None

        return article

    async def _deliver(self, article: Article) -> None:
        sent_ok = await self.notifier.send_article(article)
        if sent_ok:
            await self.db.mark_sent(article)
            self.stats.total_sent += 1
            log.info("Sent [%s] '%s' (%s)", article.category, article.title, article.source)
        else:
            self.stats.errors += 1
            log.warning("Delivery failed: %s", article.url)


# =============================================================================
# SECTION 13: BOT COMMANDS + BUTTONS
# =============================================================================

UNAUTHORIZED_MESSAGE = "You are not authorized."


def owner_only(handler):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if user is None or user.id != _OWNER_ID:
            if update.effective_message:
                await update.effective_message.reply_text(UNAUTHORIZED_MESSAGE)
            elif update.callback_query:
                await update.callback_query.answer(UNAUTHORIZED_MESSAGE, show_alert=True)
            log.warning("Unauthorized access from user_id=%s",
                        user.id if user else "unknown")
            return
        return await handler(update, context)
    return wrapper


def main_menu_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("Start Monitoring",  callback_data="start_monitor"),
         InlineKeyboardButton("Stop Monitoring",   callback_data="stop_monitor")],
        [InlineKeyboardButton("Stats",             callback_data="stats"),
         InlineKeyboardButton("Sources",           callback_data="sources")],
        [InlineKeyboardButton("Refresh Now",       callback_data="refresh"),
         InlineKeyboardButton("Settings",          callback_data="settings")],
    ]
    return InlineKeyboardMarkup(rows)


def _start_text(monitor: MonitorService) -> str:
    status = "Running" if monitor.stats.monitoring_enabled else "Stopped"
    return (
        "Crypto News Bot\n\n"
        f"Status: {status}\n\n"
        "Priority order:\n" +
        "\n".join(f"  {i+1}. {c}" for i, c in enumerate(CATEGORIES))
    )


@owner_only
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    monitor: MonitorService = context.bot_data["monitor"]
    await update.effective_message.reply_text(
        _start_text(monitor), reply_markup=main_menu_keyboard())


def _status_text(monitor: MonitorService, scheduler: AsyncIOScheduler) -> str:
    s = monitor.stats
    last = (s.last_check_time.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            if s.last_check_time else "Never")
    job = scheduler.get_job("monitor_job")
    nxt = (job.next_run_time.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
           if job and job.next_run_time else "N/A")
    return (
        "Bot Status\n\n"
        "Running: Yes\n"
        f"Checked:   {s.total_checked}\n"
        f"Sent:      {s.total_sent}\n"
        f"Too old:   {s.too_old_skipped}\n"
        f"Last:      {last}\n"
        f"Next:      {nxt}\n"
        f"Monitor:   {'On' if s.monitoring_enabled else 'Off'}"
    )


@owner_only
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    monitor: MonitorService     = context.bot_data["monitor"]
    scheduler: AsyncIOScheduler = context.bot_data["scheduler"]
    await update.effective_message.reply_text(_status_text(monitor, scheduler))


@owner_only
async def cmd_start_monitor(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    monitor: MonitorService = context.bot_data["monitor"]
    monitor.stats.monitoring_enabled = True
    log.info("Monitoring ENABLED by owner.")
    await update.effective_message.reply_text(
        f"Monitoring started (every {monitor.cfg.poll_interval}s, last {_MAX_AGE_HOURS}h only).")


@owner_only
async def cmd_stop_monitor(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    monitor: MonitorService = context.bot_data["monitor"]
    monitor.stats.monitoring_enabled = False
    log.info("Monitoring DISABLED by owner.")
    await update.effective_message.reply_text(
        "Monitoring stopped. Use /start_monitor to resume.")


@owner_only
async def cmd_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    monitor: MonitorService = context.bot_data["monitor"]
    await update.effective_message.reply_text("Refreshing all sources now...")
    checked, sent = await monitor.run_cycle()
    await update.effective_message.reply_text(
        f"Done.\nChecked: {checked}  |  Sent: {sent}")


def _stats_text(monitor: MonitorService) -> str:
    s = monitor.stats
    return (
        "Statistics\n\n"
        f"Checked:    {s.total_checked}\n"
        f"Sent:       {s.total_sent}\n"
        f"Duplicates: {s.duplicates_ignored}\n"
        f"Too old:    {s.too_old_skipped}\n"
        f"Errors:     {s.errors}\n"
        f"Uptime:     {s.uptime_str()}"
    )


@owner_only
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    monitor: MonitorService = context.bot_data["monitor"]
    await update.effective_message.reply_text(_stats_text(monitor))


def _sources_text() -> str:
    lines = ["Monitored Sources (priority order)\n"]
    by_cat: dict[str, list[str]] = {}
    for s in SOURCES:
        by_cat.setdefault(s.category, []).append(s.name)
    for cat in CATEGORIES:
        names = by_cat.get(cat, [])
        if names:
            lines.append(f"\n{CATEGORY_PRIORITY[cat]}. {cat}:")
            lines.extend(f"  - {n}" for n in names)
    return "\n".join(lines)


def _settings_text(cfg: Config) -> str:
    ai = "Enabled (OpenAI)" if cfg.openai_api_key else "Disabled (extractive only)"
    return (
        "Settings\n\n"
        f"Poll interval:    {cfg.poll_interval}s\n"
        f"Max news age:     {_MAX_AGE_HOURS}h\n"
        f"Summary length:   30-70 words\n"
        f"Max concurrent:   {cfg.max_concurrent_fetches}\n"
        f"Timeout:          {cfg.request_timeout}s\n"
        f"AI summaries:     {ai}\n"
        f"Database:         {cfg.db_path}"
    )


@owner_only
async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    monitor: MonitorService = context.bot_data["monitor"]
    cfg: Config             = context.bot_data["cfg"]
    action = query.data

    if action == "start_monitor":
        monitor.stats.monitoring_enabled = True
        await query.edit_message_text(_start_text(monitor), reply_markup=main_menu_keyboard())
    elif action == "stop_monitor":
        monitor.stats.monitoring_enabled = False
        await query.edit_message_text(_start_text(monitor), reply_markup=main_menu_keyboard())
    elif action == "stats":
        await query.message.reply_text(_stats_text(monitor))
    elif action == "sources":
        await query.message.reply_text(_sources_text())
    elif action == "refresh":
        await query.message.reply_text("Refreshing all sources...")
        checked, sent = await monitor.run_cycle()
        await query.message.reply_text(f"Done.\nChecked: {checked}  |  Sent: {sent}")
    elif action == "settings":
        await query.message.reply_text(_settings_text(cfg))
    else:
        log.warning("Unknown button: %s", action)


@owner_only
async def on_unrecognized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        "Unrecognized command. Use /start to open the menu.")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.error("Unhandled error: %s", context.error, exc_info=context.error)
    monitor: Optional[MonitorService] = context.bot_data.get("monitor")
    if monitor:
        monitor.stats.errors += 1


# =============================================================================
# SECTION 14: SCHEDULER
# =============================================================================

async def scheduled_check(application: Application) -> None:
    monitor: MonitorService = application.bot_data["monitor"]
    if not monitor.stats.monitoring_enabled:
        return
    try:
        checked, sent = await monitor.run_cycle()
        if checked or sent:
            log.info("Scheduled check: checked=%d sent=%d", checked, sent)
    except Exception as exc:
        monitor.stats.errors += 1
        log.error("Scheduled check failed: %s", exc, exc_info=True)


async def post_init(application: Application) -> None:
    cfg: Config = application.bot_data["cfg"]
    setup_logging(cfg)
    log.info("=== Crypto News Bot starting up ===")

    db    = Database(cfg.db_path)
    stats = Stats(db)
    await stats.load()

    connector  = aiohttp.TCPConnector(limit=cfg.max_concurrent_fetches * 3)
    session    = aiohttp.ClientSession(connector=connector)
    http       = HttpFetcher(session, cfg)
    feed_fetcher = FeedFetcher(http)
    extractor    = ArticleExtractor(http)
    summarizer   = Summarizer(http, cfg)
    notifier     = TelegramNotifier(application, cfg.owner_id, cfg)
    monitor      = MonitorService(cfg, db, feed_fetcher, extractor,
                                  summarizer, notifier, stats)

    application.bot_data.update({"db": db, "session": session, "monitor": monitor})

    scheduler = AsyncIOScheduler(timezone=timezone.utc)
    scheduler.add_job(
        scheduled_check,
        trigger=IntervalTrigger(seconds=cfg.poll_interval),
        args=[application],
        id="monitor_job",
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(timezone.utc),
    )
    scheduler.start()
    application.bot_data["scheduler"] = scheduler

    log.info("Monitoring %d sources every %ds (last %dh only).",
             len(SOURCES), cfg.poll_interval, _MAX_AGE_HOURS)

    with contextlib.suppress(TelegramError):
        await application.bot.send_message(
            chat_id=cfg.owner_id,
            text=(
                "Crypto News Bot is online!\n\n"
                f"Priority: Telegram -> TON -> Crypto -> NFT -> Exchange -> Security -> Scam\n"
                f"News age filter: last {_MAX_AGE_HOURS}h only\n"
                "Summary length: 30-70 words\n"
                "Hashtags: enabled\n\n"
                "Use /start to open the menu."
            ),
        )


async def post_shutdown(application: Application) -> None:
    log.info("Shutting down...")
    scheduler: Optional[AsyncIOScheduler] = application.bot_data.get("scheduler")
    if scheduler:
        scheduler.shutdown(wait=False)
    session: Optional[aiohttp.ClientSession] = application.bot_data.get("session")
    if session:
        await session.close()
    log.info("=== Crypto News Bot stopped ===")


# =============================================================================
# SECTION 15: APPLICATION BOOTSTRAP
# =============================================================================

def build_application(cfg: Config) -> Application:
    request = HTTPXRequest(
        connect_timeout=cfg.request_timeout,
        read_timeout=cfg.request_timeout,
    )
    app = (
        ApplicationBuilder()
        .token(cfg.bot_token)
        .request(request)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    app.bot_data["cfg"] = cfg

    owner_filter = filters.User(user_id=cfg.owner_id)

    app.add_handler(CommandHandler("start",         cmd_start,         filters=owner_filter))
    app.add_handler(CommandHandler("status",        cmd_status,        filters=owner_filter))
    app.add_handler(CommandHandler("start_monitor", cmd_start_monitor, filters=owner_filter))
    app.add_handler(CommandHandler("stop_monitor",  cmd_stop_monitor,  filters=owner_filter))
    app.add_handler(CommandHandler("refresh",       cmd_refresh,       filters=owner_filter))
    app.add_handler(CommandHandler("stats",         cmd_stats,         filters=owner_filter))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(CommandHandler("start", on_unrecognized, filters=~owner_filter))
    app.add_handler(MessageHandler(filters.ALL & ~owner_filter, on_unrecognized))
    app.add_handler(MessageHandler(filters.COMMAND & owner_filter, on_unrecognized))
    app.add_handler(MessageHandler(filters.ALL & owner_filter, on_unrecognized))
    app.add_error_handler(on_error)
    return app


def main() -> None:
    try:
        cfg = Config()
    except RuntimeError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        sys.exit(1)
    app = build_application(cfg)
    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES, close_loop=True)
    except (KeyboardInterrupt, SystemExit):
        log.info("Shutdown requested.")


if __name__ == "__main__":
    main()
