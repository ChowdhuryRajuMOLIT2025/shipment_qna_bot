import json
import os
from typing import Any, Dict, List, Optional

import feedparser
import requests

from shipment_qna_bot.logging.logger import logger


class NewsTool:
    """
    Tool to fetch logistics-related news to analyze impact on shipments.
    Primary: NewsCatcher API
    Backup: Google News RSS
    """

    GOOGLE_NEWS_RSS_URL = (
        "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
    )
    NEWSCATCHER_URL = "https://api.newscatcherapi.com/v2/search"

    def __init__(self):
        self.newscatcher_api_key = os.getenv("NEWSCATCHER_API_KEY")

    def fetch_news(self, keywords: List[str], limit: int = 5) -> List[Dict[str, Any]]:
        """
        Fetches news from primary source, fails over to backup if needed.
        """
        if not keywords:
            return []

        query = " OR ".join(keywords)

        # Try NewsCatcher first if API key is present
        if self.newscatcher_api_key:
            try:
                news = self._fetch_from_newscatcher(query, limit)
                if news:
                    logger.info(
                        f"Successfully fetched {len(news)} articles from NewsCatcher."
                    )
                    return news
            except Exception as e:
                logger.warning(f"NewsCatcher failed, falling back to Google News: {e}")

        # Fallback to Google News RSS
        try:
            news = self._fetch_from_google_news(query, limit)
            logger.info(
                f"Successfully fetched {len(news)} articles from Google News RSS."
            )
            return news
        except Exception as e:
            logger.error(f"Google News RSS fetch failed: {e}")
            return []

    def _fetch_from_newscatcher(self, query: str, limit: int) -> List[Dict[str, Any]]:
        headers = {"x-api-key": self.newscatcher_api_key}
        params = {"q": query, "lang": "en", "page_size": limit, "topic": "business"}
        resp = requests.get(
            self.NEWSCATCHER_URL, headers=headers, params=params, timeout=10
        )
        resp.raise_for_status()
        data = resp.json()

        results = []
        for art in data.get("articles", []):
            results.append(
                {
                    "title": art.get("title"),
                    "link": art.get("link"),
                    "source": art.get("clean_url"),
                    "published": art.get("published_date"),
                    "summary": art.get("summary"),
                }
            )
        return results

    def _fetch_from_google_news(self, query: str, limit: int) -> List[Dict[str, Any]]:
        import urllib.parse

        encoded_query = urllib.parse.quote(query)
        url = self.GOOGLE_NEWS_RSS_URL.format(query=encoded_query)

        feed = feedparser.parse(url)
        results = []
        for entry in feed.entries[:limit]:
            results.append(
                {
                    "title": entry.get("title"),
                    "link": entry.get("link"),
                    "source": entry.get("source", {}).get("title", "Google News"),
                    "published": entry.get("published"),
                    "summary": entry.get("summary", ""),
                }
            )
        return results
