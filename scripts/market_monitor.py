#!/usr/bin/env python3
"""
Market Intelligence Monitor
============================
Daily: fetches last 24h of content from financial analysts, extracts insights
       via Claude, corroborates across sources, emails a digest.
Weekly (Sundays): synthesises the past 7 daily JSON files into a weekly report.

Sources monitored
-----------------
YouTube : Thoughtful Money, Eurodollar University, All-In Podcast,
          George Gammon, Forward Guidance (Jack Farley)
RSS     : Lyn Alden, Doomberg, SemiAnalysis, Macro Voices, Kitco News,
          Real Investment Advice (Lance Roberts), Sprott, Grant Williams,
          Odd Lots, Stratechery, Macleod Finance, The Diff
Earnings: Google News RSS for AI/tech, precious metals miners, industrial commodities
"""

import os
import json
import re
import time
import datetime
import smtplib
import subprocess
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

import requests
import feedparser
import anthropic
# Transcripts fetched via yt-dlp (see get_youtube_transcript)

# ── Configuration ─────────────────────────────────────────────────────────────

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
YAHOO_EMAIL       = os.environ["YAHOO_EMAIL"]
YAHOO_PASSWORD    = os.environ["YAHOO_APP_PASSWORD"]
RECIPIENT_EMAIL   = os.environ["RECIPIENT_EMAIL"]

claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

TODAY     = datetime.date.today()
IS_WEEKLY = TODAY.weekday() == 6  # Sunday

BASE_DIR   = Path(__file__).parent.parent
DAILY_DIR  = BASE_DIR / "data" / "daily"
WEEKLY_DIR = BASE_DIR / "data" / "weekly"
CALLS_FILE = BASE_DIR / "data" / "open_calls.json"

DAILY_DIR.mkdir(parents=True, exist_ok=True)
WEEKLY_DIR.mkdir(parents=True, exist_ok=True)

MAX_TRANSCRIPT_CHARS = 14000   # ~3 500 Claude tokens
FETCH_DAYS_BACK      = 3       # rolling fetch window (content deduped against prior runs)
MAX_ARTICLE_CHARS    = 8000

# ── Sources ───────────────────────────────────────────────────────────────────

YOUTUBE_CHANNELS = {
    "Thoughtful Money":        "https://www.youtube.com/@adam.taggart/videos",
    "Thoughtful Money (Live)": "https://www.youtube.com/@adam.taggart/streams",
    "Eurodollar University":   "https://www.youtube.com/@EurodollarUniversity",
    "All-In Podcast":          "https://www.youtube.com/@allin",
    "George Gammon":           "https://www.youtube.com/@GeorgeGammon",
    "Forward Guidance":        "https://www.youtube.com/@ForwardGuidance",
}

RSS_SOURCES = {
    "Lyn Alden":              "https://www.lynalden.com/feed/",
    "Doomberg":               "https://doomberg.substack.com/feed",
    "SemiAnalysis":           "https://semianalysis.substack.com/feed",
    "Macro Voices":           "https://www.macrovoices.com/feed",
    "Kitco News":             "https://www.kitco.com/rss/kitco-news.rss",
    "Real Investment Advice": "https://realinvestmentadvice.com/feed/",
    "Sprott":                 "https://sprott.com/feed/",
    "Grant Williams":         "https://www.ttmygh.com/feed/",
    # New sources
    "Odd Lots":               "https://www.omnycontent.com/d/playlist/e73c998e-6e60-432f-8610-ae210140c5b1/8a94442e-5a74-4fa2-8b8d-ae27003a8d6b/982f5071-765c-403d-969d-ae27003a8d83/podcast.rss",  # confirmed via Apple Podcasts API
    "Stratechery":            "https://stratechery.com/feed/",
    "Macleod Finance":        "https://alasdairmacleod.substack.com/feed",  # confirmed
    "The Diff":               "https://www.thediff.co/rss/",               # confirmed
}

# Luke Gromen (FFTT) posts primarily behind a paid newsletter and on X.
# Add his RSS here if you subscribe: e.g. "Luke Gromen": "https://fftt-llc.com/feed/"

# Earnings & company news via Google News RSS – one feed per sector theme.
# Google News search RSS is reliably public; update the search terms as needed.
EARNINGS_SOURCES = {
    "AI/Tech Earnings":
        "https://news.google.com/rss/search?q=NVDA+OR+AMD+OR+Microsoft+OR+TSMC+earnings+results&hl=en-US&gl=US&ceid=US:en",
    "Precious Metals Earnings":
        "https://news.google.com/rss/search?q=Newmont+OR+Barrick+OR+Agnico+Eagle+OR+Wheaton+gold+miner+earnings&hl=en-US&gl=US&ceid=US:en",
    "Industrial Commodities Earnings":
        "https://news.google.com/rss/search?q=Freeport+OR+BHP+OR+Rio+Tinto+OR+Caterpillar+copper+commodity+earnings&hl=en-US&gl=US&ceid=US:en",
}

# Key instruments to display as a live price snapshot at the top of every email.
# Tickers must be valid Yahoo Finance symbols.
PRICE_WATCHLIST = [
    # (symbol,  display name,          sector)
    ("NVDA",  "Nvidia",               "ai_tech"),
    ("QQQ",   "Nasdaq 100",           "ai_tech"),
    ("SOXX",  "Semiconductors",       "ai_tech"),
    ("GLD",   "Gold",                 "precious_metals"),
    ("GDX",   "Gold Miners",          "precious_metals"),
    ("SLV",   "Silver",               "precious_metals"),
    ("COPX",  "Copper Miners",        "industrial_commodities"),
    ("XME",   "Metals & Mining",      "industrial_commodities"),
    ("TLT",   "20yr Bonds",           "macro"),
    ("UUP",   "US Dollar",            "macro"),
]

# X / Twitter sources via RSSHub.  Works out-of-the-box with the public instance
# (rsshub.app) or a self-hosted one — set RSSHUB_BASE in GitHub Secrets to switch.
# Free self-hosting on Vercel: https://docs.rsshub.app/deploy/
_RSSHUB_BASE = os.environ.get("RSSHUB_BASE", "https://rsshub.app")

X_SOURCES = {
    "Luke Gromen (X)":   f"{_RSSHUB_BASE}/twitter/user/LukeGromen",
    "Jeff Snider (X)":   f"{_RSSHUB_BASE}/twitter/user/JeffSnider_AIP",
    "Doomberg (X)":      f"{_RSSHUB_BASE}/twitter/user/doombergT",
    "Lyn Alden (X)":     f"{_RSSHUB_BASE}/twitter/user/LynAldenContact",
    "Adam Taggart (X)":  f"{_RSSHUB_BASE}/twitter/user/AdamTaggart_TTM",
}

# Keywords that flag an article as earnings-relevant (title match, case-insensitive)
_EARNINGS_KEYWORDS = frozenset({
    "earnings", "revenue", "guidance", "outlook", "q1", "q2", "q3", "q4",
    "beats", "misses", "eps", "results", "quarterly", "transcript", "profit",
    "forecast", "raised", "lowered",
})

# ── Content Fetching ──────────────────────────────────────────────────────────

def get_recent_youtube_videos(channel_url: str, source_name: str,
                               days_back: int = 1) -> list[dict]:
    """Return video metadata for videos published in the last `days_back` days."""
    cutoff = (TODAY - datetime.timedelta(days=days_back)).strftime("%Y%m%d")
    try:
        result = subprocess.run(
            [
                "yt-dlp", "--flat-playlist", "--playlist-end", "15",
                "--dateafter", cutoff,   # yt-dlp native filter: skip older videos
                "--print", "%(id)s|||%(title)s|||%(upload_date)s",
                "--no-warnings", "--quiet", channel_url,
            ],
            capture_output=True, text=True, timeout=90,
        )
        videos = []
        for line in result.stdout.strip().splitlines():
            if "|||" not in line:
                continue
            parts = line.split("|||")
            if len(parts) < 3:
                continue
            vid_id, title, upload_date = (p.strip() for p in parts[:3])
            # Belt-and-suspenders: also validate date format before comparing
            if re.match(r"^\d{8}$", upload_date or "") and upload_date >= cutoff:
                videos.append({
                    "id":     vid_id,
                    "title":  title,
                    "date":   f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}",
                    "url":    f"https://youtube.com/watch?v={vid_id}",
                    "source": source_name,
                })
        return videos
    except Exception as exc:
        print(f"  ⚠  YouTube fetch failed for {source_name}: {exc}")
        return []


def get_youtube_transcript(video_id: str, title: str) -> str | None:
    """Fetch auto-generated transcript via yt-dlp VTT subtitle download.

    Uses yt-dlp (already installed for channel scraping) rather than a separate
    library, so transcript fetching stays in sync with YouTube changes.
    """
    import tempfile
    url = f"https://youtube.com/watch?v={video_id}"
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            subprocess.run(
                [
                    "yt-dlp",
                    "--write-auto-subs", "--no-download",
                    "--sub-langs", "en.*",
                    "--sub-format", "vtt",
                    "--output", f"{tmpdir}/sub",
                    "--no-warnings", "--quiet",
                    url,
                ],
                capture_output=True, text=True, timeout=90,
            )
            vtt_files = list(Path(tmpdir).glob("*.vtt"))
            if not vtt_files:
                print(f"    No transcript available: {title[:60]}")
                return None

            content = vtt_files[0].read_text(encoding="utf-8", errors="ignore")

            # Strip VTT metadata, timestamps, and inline tags; deduplicate adjacent lines
            lines, prev = [], None
            for line in content.splitlines():
                line = line.strip()
                if (not line
                        or "WEBVTT" in line
                        or "-->" in line
                        or re.match(r"^\d{2}:\d{2}", line)):
                    continue
                line = re.sub(r"<[^>]+>", "", line).strip()
                if line and line != prev:
                    lines.append(line)
                    prev = line

            text = " ".join(lines)
            if not text:
                print(f"    Empty transcript: {title[:60]}")
                return None
            return text[:MAX_TRANSCRIPT_CHARS]

    except Exception as exc:
        print(f"    Transcript error ({title[:50]}): {exc}")
        return None


def _strip_html(raw: str) -> str:
    text = re.sub(r"<[^>]+>", " ", raw)
    return re.sub(r"\s+", " ", text).strip()


def get_rss_articles(source_name: str, feed_url: str,
                     days_back: int = 1) -> list[dict]:
    """Return recent articles from an RSS feed."""
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days_back)
    articles = []
    try:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries[:15]:
            pub = None
            for attr in ("published_parsed", "updated_parsed"):
                val = getattr(entry, attr, None)
                if val:
                    pub = datetime.datetime(*val[:6], tzinfo=datetime.timezone.utc)
                    break
            if pub and pub < cutoff:
                continue

            raw = ""
            if hasattr(entry, "content"):
                raw = entry.content[0].value
            elif hasattr(entry, "summary"):
                raw = entry.summary

            content = _strip_html(raw)
            if len(content) < 120:
                continue

            articles.append({
                "source": source_name,
                "title":  getattr(entry, "title", "Untitled"),
                "url":    getattr(entry, "link", feed_url),
                "date":   pub.date().isoformat() if pub else TODAY.isoformat(),
                "content": content[:MAX_ARTICLE_CHARS],
            })
    except Exception as exc:
        print(f"  ⚠  RSS error ({source_name}): {exc}")
    return articles

# ── Market Snapshot ───────────────────────────────────────────────────────────

_YF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Accept":     "application/json",
}


def get_market_snapshot() -> list[dict]:
    """Fetch current price and 1-day % change for every symbol in PRICE_WATCHLIST.

    Tries two Yahoo Finance endpoints in case one is rate-limited.
    """
    symbols = [sym for sym, *_ in PRICE_WATCHLIST]
    sym_str = ",".join(symbols)
    urls = [
        f"https://query1.finance.yahoo.com/v7/finance/quote?lang=en-US&region=US&symbols={sym_str}",
        f"https://query2.finance.yahoo.com/v7/finance/quote?lang=en-US&region=US&symbols={sym_str}",
    ]
    for url in urls:
        try:
            resp = requests.get(url, headers=_YF_HEADERS, timeout=15)
            body = resp.json()
            result = (body.get("quoteResponse") or {}).get("result") or []
            if not result:
                continue
            quotes = {q["symbol"]: q for q in result}
            snap = []
            for sym, name, sector in PRICE_WATCHLIST:
                q = quotes.get(sym, {})
                snap.append({
                    "symbol":     sym,
                    "name":       name,
                    "sector":     sector,
                    "price":      q.get("regularMarketPrice"),
                    "change_pct": q.get("regularMarketChangePercent"),
                })
            print(f"  📈 Price snapshot: {len([s for s in snap if s['price']])} ticker(s) fetched")
            return snap
        except Exception as exc:
            print(f"  ⚠  Price snapshot attempt failed ({url[-20:]}): {exc}")
    print("  ⚠  Price snapshot unavailable — continuing without it")
    return []


# ── Deduplication ────────────────────────────────────────────────────────────

def get_already_processed_urls() -> set[str]:
    """Return URLs already processed in the previous (FETCH_DAYS_BACK - 1) daily runs.

    With a 72h fetch window we'd otherwise re-process yesterday's and the day
    before's content every day.  Reading source_url from recent daily JSONs lets
    us skip anything already seen — so each item is only processed once, on the
    first day it falls inside the window.
    """
    seen: set[str] = set()
    for i in range(1, FETCH_DAYS_BACK):          # e.g. 1 and 2 for a 3-day window
        fp = DAILY_DIR / f"{(TODAY - datetime.timedelta(days=i)).isoformat()}.json"
        if fp.exists():
            try:
                data = json.loads(fp.read_text())
                for ins in data.get("insights", []):
                    url = ins.get("source_url", "")
                    if url:
                        seen.add(url)
            except Exception:
                pass
    return seen


# ── Sentiment Trends ─────────────────────────────────────────────────────────

def get_sentiment_trends(days: int = 14) -> dict[str, list[float | None]]:
    """Read the last `days` daily JSONs (oldest→newest) and return per-sector scores.

    Returns a dict like {"ai_tech": [None, 0.2, 0.4, ...], ...}.
    None means no data that day (no content published / first run).
    """
    sectors = ("ai_tech", "precious_metals", "industrial_commodities", "macro")
    trends: dict[str, list[float | None]] = {s: [] for s in sectors}
    for i in range(days - 1, -1, -1):   # oldest first
        fp = DAILY_DIR / f"{(TODAY - datetime.timedelta(days=i)).isoformat()}.json"
        if fp.exists():
            try:
                dash = json.loads(fp.read_text()).get("dashboard", {}).get("sectors", {})
                for s in sectors:
                    score = dash.get(s, {}).get("sentiment_score")
                    trends[s].append(float(score) if score is not None else None)
            except Exception:
                for s in sectors: trends[s].append(None)
        else:
            for s in sectors: trends[s].append(None)
    return trends


def _trend_indicator(scores: list[float | None]) -> str:
    """Return a compact 'arrow + delta' string for the recent trend."""
    valid = [(i, s) for i, s in enumerate(scores) if s is not None]
    if len(valid) < 2:
        return ""
    first_score = valid[0][1]
    last_score  = valid[-1][1]
    delta = last_score - first_score
    if   delta >  0.15: arrow, col = "↑", "#27ae60"
    elif delta < -0.15: arrow, col = "↓", "#c0392b"
    else:               arrow, col = "→", "#888"
    return (
        f'<span style="font-size:10px;color:{col};font-weight:600">'
        f'{arrow} {delta:+.2f} (14d)</span>'
    )


# ── Insight Extraction ────────────────────────────────────────────────────────

_EXTRACT_SYSTEM = """\
You are a financial intelligence analyst for a retail investor focused on ETFs
in AI/tech, precious metals, and industrial commodities.

Extract specific, forward-looking market insights from the content supplied.
Ignore vague commentary, pure news summaries, or retrospective analysis.
Focus on concrete calls, price targets, forecasts, and directional views.

Always return valid JSON exactly matching the schema requested."""

_EXTRACT_PROMPT = """\
Source      : {source}
Content type: {content_type}
Title       : {title}
Date        : {date}
URL         : {url}

Content:
{content}

---
Extract all specific, forward-looking market insights. Return JSON:

{{
  "insights": [
    {{
      "summary":          "1-2 sentence plain-English summary of the call",
      "asset_classes":    ["one or more of: ai_tech, precious_metals, industrial_commodities, energy, bonds, currencies, macro, crypto, other"],
      "instruments":      ["specific tickers, ETFs, or named assets — e.g. GLD, NVDA, copper, gold, QQQ"],
      "direction":        "bullish | bearish | neutral | mixed",
      "timeframe":        "stated timeframe string, or null if not given",
      "specificity":      "high | medium | low",
      "key_reasoning":    "1-2 sentences on the core supporting argument",
      "notable_data":     "specific numbers, price levels, or statistics cited, or null",
      "source_confidence": 0.0
    }}
  ],
  "skipped_reason": "brief note if no actionable insights found, else null"
}}

source_confidence scoring guide (0.0 – 1.0):
  0.8-1.0 : specific price target AND timeframe, strong supporting data
  0.5-0.7 : clear directional view with rough timeframe or data
  0.2-0.4 : directional view, vague or no timeframe
  0.0-0.1 : speculative / very general"""


def extract_insights(item: dict, content_type: str) -> list[dict]:
    """Call Claude to pull structured insights from a single piece of content."""
    content = item.get("content") or item.get("transcript", "")
    if not content or len(content) < 150:
        return []

    prompt = _EXTRACT_PROMPT.format(
        source=item["source"],
        content_type=content_type,
        title=item.get("title", "N/A"),
        date=item.get("date", TODAY.isoformat()),
        url=item.get("url", "N/A"),
        content=content,
    )
    for attempt in range(2):
        try:
            resp = claude.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=2000,
                system=_EXTRACT_SYSTEM,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp.content[0].text.strip()
            m = re.search(r"\{.*\}", raw, re.DOTALL)
            if m:
                raw = m.group(0)
            data = json.loads(raw)
            insights = data.get("insights", [])
            for ins in insights:
                ins.update({
                    "source":        item["source"],
                    "source_url":    item.get("url", ""),
                    "content_title": item.get("title", ""),
                    "content_date":  item.get("date", TODAY.isoformat()),
                    "content_type":  content_type,
                    "corroboration_confidence": 0.0,
                    "corroborated_by": [],
                })
            return insights
        except json.JSONDecodeError as exc:
            if attempt == 0:
                print(f"    JSON parse error, retrying ({item.get('title', '')[:40]}): {exc}")
                time.sleep(2)
                continue
            print(f"    Extraction error after retry ({item.get('title', '')[:40]}): {exc}")
        except Exception as exc:
            print(f"    Extraction error ({item.get('title', '')[:50]}): {exc}")
        return []

# ── Corroboration ─────────────────────────────────────────────────────────────

_CORROBORATE_SYSTEM = """\
You are a financial intelligence analyst. Given insights extracted from multiple
independent sources, identify convergence and assign corroboration scores."""

_CORROBORATE_PROMPT = """\
Below are today's market insights from multiple independent sources.

For each insight (identified by its index), assign a corroboration_confidence
score (0.0 – 1.0) based on how many *independent* sources express a similar
directional view on the same asset or theme.

Also identify the top convergence themes (≥ 2 sources agreeing).

Insights:
{insights_json}

Return JSON:
{{
  "scored_insights": [
    {{
      "index": 0,
      "corroboration_confidence": 0.0,
      "corroborated_by": ["Source Name", ...]
    }}
  ],
  "convergence_themes": [
    {{
      "theme":              "brief description",
      "direction":          "bullish | bearish | neutral",
      "asset_classes":      ["..."],
      "supporting_sources": ["..."],
      "strength":           "strong | moderate | weak"
    }}
  ]
}}"""


def corroborate_insights(all_insights: list[dict]) -> tuple[list[dict], list[dict]]:
    """Score cross-source corroboration and return (scored_insights, convergence_themes)."""
    if len(all_insights) < 2:
        return all_insights, []

    slim = [
        {
            "index":        i,
            "source":       ins["source"],
            "summary":      ins["summary"],
            "direction":    ins["direction"],
            "asset_classes": ins.get("asset_classes", []),
            "instruments":  ins.get("instruments", []),
        }
        for i, ins in enumerate(all_insights)
    ]
    prompt = _CORROBORATE_PROMPT.format(insights_json=json.dumps(slim, indent=2))

    try:
        resp = claude.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2500,
            system=_CORROBORATE_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            raw = m.group(0)
        data = json.loads(raw)

        for scored in data.get("scored_insights", []):
            idx = scored.get("index")
            if idx is not None and 0 <= idx < len(all_insights):
                all_insights[idx]["corroboration_confidence"] = scored.get(
                    "corroboration_confidence", 0.0)
                all_insights[idx]["corroborated_by"] = scored.get("corroborated_by", [])

        return all_insights, data.get("convergence_themes", [])

    except Exception as exc:
        print(f"  ⚠  Corroboration error: {exc}")
        return all_insights, []

# ── Daily Dashboard Synthesis ─────────────────────────────────────────────────

_DASHBOARD_SYSTEM = """\
You are a senior financial analyst producing a concise daily market dashboard for a
retail investor focused on ETFs in AI/tech, precious metals, and industrial commodities."""

_DASHBOARD_PROMPT = """\
Today's extracted market insights and convergence themes are below.

Insights ({n_insights} total):
{insights_json}

Convergence themes:
{themes_json}

Produce a dashboard synthesis. Return JSON:

{{
  "market_summary": "One tight paragraph (3-5 sentences) summarising today's overall market picture — what matters most for an investor in AI/tech, precious metals, and industrial commodities.",
  "sectors": {{
    "ai_tech": {{
      "sentiment_score": 0.0,
      "recommendation": "buy | hold | sell | no signal",
      "rationale": "1-2 sentence explanation of the call"
    }},
    "precious_metals": {{
      "sentiment_score": 0.0,
      "recommendation": "buy | hold | sell | no signal",
      "rationale": "1-2 sentence explanation"
    }},
    "industrial_commodities": {{
      "sentiment_score": 0.0,
      "recommendation": "buy | hold | sell | no signal",
      "rationale": "1-2 sentence explanation"
    }},
    "macro": {{
      "sentiment_score": 0.0,
      "recommendation": "buy | hold | sell | no signal",
      "rationale": "1-2 sentence explanation"
    }}
  }}
}}

sentiment_score: -1.0 = strongly bearish, 0.0 = neutral, +1.0 = strongly bullish.
Use "no signal" when there is insufficient data for a sector today.
Base recommendations on the weight of evidence across all sources — not a single data point."""


def synthesize_daily_dashboard(insights: list[dict], themes: list[dict]) -> dict:
    """One Claude call that produces the market summary paragraph and sector signals."""
    if not insights:
        return {}

    slim = [
        {
            "source":        ins["source"],
            "summary":       ins["summary"],
            "direction":     ins["direction"],
            "asset_classes": ins.get("asset_classes", []),
            "source_confidence":        ins.get("source_confidence", 0.0),
            "corroboration_confidence": ins.get("corroboration_confidence", 0.0),
        }
        for ins in insights
    ]
    prompt = _DASHBOARD_PROMPT.format(
        n_insights=len(insights),
        insights_json=json.dumps(slim, indent=2),
        themes_json=json.dumps(themes, indent=2),
    )
    try:
        resp = claude.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1200,
            system=_DASHBOARD_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        return json.loads(m.group(0)) if m else {}
    except Exception as exc:
        print(f"  ⚠  Dashboard synthesis error: {exc}")
        return {}


# ── Open Calls Tracker ────────────────────────────────────────────────────────

def _load_open_calls() -> list[dict]:
    if CALLS_FILE.exists():
        return json.loads(CALLS_FILE.read_text())
    return []


def _save_open_calls(calls: list[dict]):
    CALLS_FILE.write_text(json.dumps(calls, indent=2))


def _parse_timeframe(timeframe: str, date_made: str) -> datetime.date | None:
    """Best-effort parse of a timeframe string into an expected resolution date."""
    tf   = timeframe.lower().strip()
    base = datetime.date.fromisoformat(date_made)

    # "X week(s) / month(s) / year(s)"
    m = re.match(r"(\d+)\s*(week|month|year)", tf)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if unit == "week":   return base + datetime.timedelta(weeks=n)
        if unit == "month":  return base + datetime.timedelta(days=n * 30)
        if unit == "year":   return base + datetime.timedelta(days=n * 365)

    # "X-Y months" — use midpoint
    m = re.match(r"(\d+)-(\d+)\s*month", tf)
    if m:
        mid = (int(m.group(1)) + int(m.group(2))) // 2
        return base + datetime.timedelta(days=mid * 30)

    # "Q1/Q2/Q3/Q4 YYYY"
    m = re.match(r"q([1-4])\s*(\d{4})", tf)
    if m:
        q, yr = int(m.group(1)), int(m.group(2))
        return datetime.date(yr, q * 3, 28)   # approximate end-of-quarter

    # "end of YYYY" or bare "YYYY"
    m = re.match(r"(?:end of\s*)?(\d{4})", tf)
    if m:
        return datetime.date(int(m.group(1)), 12, 31)

    return None   # unparseable — call stays open indefinitely


def update_open_calls(new_insights: list[dict],
                      snapshot: list[dict] | None = None) -> list[dict]:
    """Log high-specificity insights with timeframes; capture entry prices when available."""
    calls    = _load_open_calls()
    existing = {c["summary"] for c in calls}
    price_map = {s["symbol"].upper(): s["price"]
                 for s in (snapshot or []) if s.get("price")}

    for ins in new_insights:
        if (ins.get("specificity") == "high"
                and ins.get("timeframe")
                and ins["summary"] not in existing):

            # Capture current price for every instrument we track
            entry_prices = {
                instr.upper(): price_map[instr.upper()]
                for instr in ins.get("instruments", [])
                if instr.upper() in price_map
            }
            calls.append({
                "summary":            ins["summary"],
                "source":             ins["source"],
                "source_url":         ins.get("source_url", ""),
                "direction":          ins["direction"],
                "instruments":        ins.get("instruments", []),
                "timeframe":          ins["timeframe"],
                "date_made":          ins["content_date"],
                "source_confidence":  ins.get("source_confidence", 0.0),
                "status":             "open",
                "entry_prices":       entry_prices,
            })
            existing.add(ins["summary"])

    _save_open_calls(calls)
    return [c for c in calls if c["status"] == "open"]


def resolve_expired_calls(snapshot: list[dict]) -> list[dict]:
    """Check open calls whose timeframe has elapsed; mark correct / incorrect.

    Returns a list of calls newly resolved in this run (for the email digest).
    """
    calls     = _load_open_calls()
    price_map = {s["symbol"].upper(): s["price"]
                 for s in snapshot if s.get("price")}
    resolved  = []

    for call in calls:
        if call.get("status") != "open":
            continue
        resolution_date = _parse_timeframe(
            call.get("timeframe", ""), call.get("date_made", TODAY.isoformat())
        )
        if not resolution_date or TODAY < resolution_date:
            continue    # not due yet

        direction     = call.get("direction", "neutral")
        entry_prices  = call.get("entry_prices", {})
        verdict       = None

        for instr in call.get("instruments", []):
            sym     = instr.upper()
            entry   = entry_prices.get(sym)
            current = price_map.get(sym)
            if entry and current:
                change = (current - entry) / entry
                correct = (change > 0.02 and direction == "bullish") or \
                          (change < -0.02 and direction == "bearish")
                verdict = "correct" if correct else "incorrect"
                call["resolution_change_pct"] = round(change * 100, 1)
                call["resolution_price"]      = current
                break

        if verdict:
            call["status"]          = verdict
            call["resolution_date"] = TODAY.isoformat()
            resolved.append(call)
        elif resolution_date < TODAY - datetime.timedelta(days=30):
            # Timeframe long past but no price data — mark unresolvable
            call["status"] = "unresolvable"

    _save_open_calls(calls)
    return resolved

# ── Weekly Synthesis ──────────────────────────────────────────────────────────

_WEEKLY_SYSTEM = """\
You are a senior financial analyst producing a weekly briefing for a retail
investor focused on AI/tech, precious metals, and industrial commodity ETFs."""

_WEEKLY_PROMPT = """\
Here are the daily market intelligence digests from the past 7 days:

{daily_summaries}

Synthesise them into a structured weekly report. Return JSON:

{{
  "week_summary": "3-4 sentence plain-English overview suitable for a retail investor",
  "dominant_themes": [
    {{"theme": "...", "direction": "bullish|bearish|neutral", "frequency": "how many days / sources"}}
  ],
  "asset_class_outlook": {{
    "ai_tech":                {{"consensus": "bullish|bearish|neutral|divided", "summary": "..."}},
    "precious_metals":        {{"consensus": "bullish|bearish|neutral|divided", "summary": "..."}},
    "industrial_commodities": {{"consensus": "bullish|bearish|neutral|divided", "summary": "..."}},
    "macro":                  {{"consensus": "bullish|bearish|neutral|divided", "summary": "..."}}
  }},
  "notable_calls": [
    {{"source": "...", "summary": "...", "direction": "...", "timeframe": "...", "confidence": 0.0}}
  ],
  "source_divergences": [
    {{"topic": "...", "camp_a": {{"sources": [], "view": "..."}}, "camp_b": {{"sources": [], "view": "..."}}}}
  ],
  "key_data_points": ["..."]
}}"""


def generate_weekly_synthesis() -> dict:
    """Read the last 7 daily JSONs and produce a weekly synthesis."""
    summaries = []
    for i in range(7):
        fp = DAILY_DIR / f"{(TODAY - datetime.timedelta(days=i)).isoformat()}.json"
        if fp.exists():
            d = json.loads(fp.read_text())
            summaries.append({
                "date":              d["date"],
                "convergence_themes": d.get("convergence_themes", []),
                "insight_count":     len(d.get("insights", [])),
                "top_insights":      d.get("insights", [])[:12],
            })

    if not summaries:
        return {}

    try:
        resp = claude.messages.create(
            model="claude-opus-4-6",
            max_tokens=3000,
            system=_WEEKLY_SYSTEM,
            messages=[{"role": "user", "content": _WEEKLY_PROMPT.format(
                daily_summaries=json.dumps(summaries, indent=2)
            )}],
        )
        raw = resp.content[0].text.strip()
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        return json.loads(m.group(0)) if m else {}
    except Exception as exc:
        print(f"  ⚠  Weekly synthesis error: {exc}")
        return {}

# ── Email Formatting ──────────────────────────────────────────────────────────

# Maps insight asset_class values → sector panel keys
_ASSET_TO_SECTOR = {
    "ai_tech":                "ai_tech",
    "precious_metals":        "precious_metals",
    "industrial_commodities": "industrial_commodities",
    "energy":                 "macro",
    "macro":                  "macro",
    "bonds":                  "macro",
    "currencies":             "macro",
    "crypto":                 "macro",
    "other":                  "macro",
}

_DIR_COLORS = {
    "bullish": ("#1a7a4a", "#e6f9f0"),
    "bearish": ("#b03030", "#fef0f0"),
    "neutral": ("#3050a0", "#eef2ff"),
    "mixed":   ("#7a5a00", "#fffbe6"),
}


def _sentiment_gauge(score: float) -> str:
    """10-segment coloured bar + label — uses only table/inline styles for email safety."""
    clamped = max(-1.0, min(1.0, score))
    filled  = round((clamped + 1.0) / 2.0 * 10)   # 0-10 segments

    # Segment palette: deep-red → orange → grey → light-green → deep-green
    palette = ["#c0392b","#c0392b","#e67e22","#f39c12","#95a5a6",
               "#95a5a6","#27ae60","#27ae60","#1e8449","#1e8449"]

    cells = "".join(
        f'<td style="background:{"" + palette[i] if i < filled else "#e0e0e0"};'
        f'height:8px;padding:0;border-right:2px solid #fff"></td>'
        for i in range(10)
    )

    if   score <= -0.7: label, lc = "STRONGLY BEARISH", "#c0392b"
    elif score <= -0.3: label, lc = "BEARISH",          "#e67e22"
    elif score <   0.3: label, lc = "NEUTRAL",          "#7f8c8d"
    elif score <   0.7: label, lc = "BULLISH",          "#27ae60"
    else:               label, lc = "STRONGLY BULLISH", "#1e8449"

    return (
        f'<table cellpadding="0" cellspacing="0" '
        f'style="width:100%;border-collapse:collapse;margin-bottom:4px">'
        f'<tr>{cells}</tr></table>'
        f'<div style="font-size:9px;font-weight:700;color:{lc};'
        f'letter-spacing:0.5px;margin-bottom:8px">{label}</div>'
    )


def _rec_badge(rec: str) -> str:
    """Buy / Hold / Sell badge."""
    styles = {
        "buy":       ("#fff", "#1a7a4a", "▲ BUY"),
        "hold":      ("#fff", "#7a5a00", "◆ HOLD"),
        "sell":      ("#fff", "#b03030", "▼ SELL"),
        "no signal": ("#888", "#e8e8e8", "— NO SIGNAL"),
    }
    fg, bg, text = styles.get(rec.lower().strip(), ("#888", "#e8e8e8", rec.upper()))
    return (
        f'<div style="margin-bottom:10px">'
        f'<span style="background:{bg};color:{fg};font-size:11px;font-weight:700;'
        f'padding:4px 12px;border-radius:3px;letter-spacing:0.5px">{text}</span>'
        f'</div>'
    )


def _sector_card(title: str, icon: str, sector_data: dict,
                 sector_insights: list[dict],
                 trend_scores: list[float | None] | None = None) -> str:
    """One sector panel (50% wide table cell)."""
    score = sector_data.get("sentiment_score", 0.0)
    rec   = sector_data.get("recommendation", "no signal")
    rat   = sector_data.get("rationale", "")

    html = (
        f'<td style="width:50%;vertical-align:top;padding:6px">'
        f'<div style="background:#fff;border-radius:8px;padding:14px 16px;'
        f'border:1px solid #e8e8e8">'
        f'<div style="display:table;width:100%;margin-bottom:10px">'
        f'<div style="display:table-cell;font-size:10px;font-weight:700;'
        f'text-transform:uppercase;letter-spacing:0.6px;color:#555">{icon}&nbsp;{title}</div>'
        + (f'<div style="display:table-cell;text-align:right">'
           + _trend_indicator(trend_scores)
           + '</div>' if trend_scores else "")
        + '</div>'
        + _sentiment_gauge(score)
        + _rec_badge(rec)
    )
    if rat:
        html += (
            f'<div style="font-size:11px;color:#666;line-height:1.45;'
            f'margin-bottom:10px;border-top:1px solid #f0f0f0;padding-top:8px">{rat}</div>'
        )

    # Up to 3 top insights for this sector
    for ins in sector_insights[:3]:
        d  = ins.get("direction", "neutral")
        fg, bg = _DIR_COLORS.get(d, ("#555", "#f5f5f5"))
        html += (
            f'<div style="border-top:1px solid #f5f5f5;padding-top:7px;margin-top:7px">'
            f'<span style="background:{bg};color:{fg};font-size:9px;font-weight:700;'
            f'padding:1px 5px;border-radius:2px;letter-spacing:0.4px">{d.upper()}</span>'
            f'<div style="font-size:11px;color:#333;line-height:1.4;margin-top:3px">'
            f'{ins["summary"]}</div>'
            f'<div style="font-size:10px;color:#aaa;margin-top:2px">{ins["source"]}</div>'
            f'</div>'
        )

    html += '</div></td>'
    return html


def _price_snapshot_html(snapshot: list[dict]) -> str:
    """Compact 2-column price table, colour-coded by 1-day change."""
    if not snapshot:
        return ""
    rows = ""
    for i in range(0, len(snapshot), 2):
        row = ""
        for item in snapshot[i:i+2]:
            price  = item.get("price")
            chg    = item.get("change_pct")
            if price is None:
                continue
            chg_s  = f"{chg:+.1f}%" if chg is not None else "—"
            chg_c  = "#27ae60" if (chg or 0) >= 0 else "#c0392b"
            row += (
                f'<td style="padding:5px 10px 5px 0;width:50%">'
                f'<span style="font-size:11px;color:#555;font-weight:600">'
                f'{item["symbol"]}</span> '
                f'<span style="font-size:11px;color:#333">${price:,.2f}</span> '
                f'<span style="font-size:10px;color:{chg_c};font-weight:600">{chg_s}</span>'
                f'</td>'
            )
        if row:
            rows += f'<tr>{row}</tr>'
    return (
        f'<div style="background:#f8f9fc;padding:14px 28px;border-bottom:1px solid #eee">'
        f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:.6px;color:#999;margin-bottom:8px">📈 Market Snapshot</div>'
        f'<table cellpadding="0" cellspacing="0" style="width:100%">{rows}</table>'
        f'</div>'
    )


def format_daily_email(data: dict) -> str:
    date_str      = data["date"]
    insights      = data.get("insights", [])
    themes        = data.get("convergence_themes", [])
    open_calls    = data.get("open_calls", [])
    resolved      = data.get("resolved_calls", [])
    dashboard     = data.get("dashboard", {})
    snapshot      = data.get("snapshot", [])
    trends        = data.get("sentiment_trends", {})
    src_count     = len(set(i["source"] for i in insights))

    # ── Group insights by sector ──────────────────────────────────────────────
    sector_insights: dict[str, list[dict]] = {
        k: [] for k in ("ai_tech", "precious_metals", "industrial_commodities", "macro")
    }
    for ins in sorted(
        insights,
        key=lambda x: x.get("source_confidence", 0) + x.get("corroboration_confidence", 0),
        reverse=True,
    ):
        placed = False
        for ac in ins.get("asset_classes", []):
            s = _ASSET_TO_SECTOR.get(ac)
            if s:
                sector_insights[s].append(ins)
                placed = True
                break
        if not placed:
            sector_insights["macro"].append(ins)

    sectors_cfg = dashboard.get("sectors", {})

    # ── Date label ────────────────────────────────────────────────────────────
    try:
        date_label = datetime.datetime.strptime(date_str, "%Y-%m-%d").strftime("%A %-d %B %Y")
    except ValueError:
        date_label = date_str

    W = "max-width:700px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont," \
        "'Segoe UI',Arial,sans-serif"

    html = (
        f'<!DOCTYPE html><html><head><meta charset="utf-8"></head>'
        f'<body style="background:#f0f2f5;margin:0;padding:20px">'
        f'<div style="{W}">'

        # ── Header ────────────────────────────────────────────────────────────
        f'<div style="background:#1a1a2e;border-radius:10px 10px 0 0;padding:22px 28px">'
        f'<div style="color:#fff;font-size:20px;font-weight:700;margin-bottom:4px">'
        f'📊 Market Intelligence Digest</div>'
        f'<div style="color:rgba(255,255,255,.55);font-size:12px">'
        f'{date_label}&nbsp;&nbsp;·&nbsp;&nbsp;{len(insights)} insights'
        f'&nbsp;&nbsp;·&nbsp;&nbsp;{src_count} sources</div>'
        f'</div>'
    )

    # ── Price Snapshot ────────────────────────────────────────────────────────
    html += _price_snapshot_html(snapshot)

    # ── Market Overview ───────────────────────────────────────────────────────
    if dashboard.get("market_summary"):
        html += (
            f'<div style="background:#fff;padding:18px 28px;border-bottom:1px solid #eee">'
            f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:.6px;color:#999;margin-bottom:10px">📋 Market Overview</div>'
            f'<div style="font-size:14px;line-height:1.65;color:#222">'
            f'{dashboard["market_summary"]}</div>'
            f'</div>'
        )

    # ── Sector Dashboard (2 × 2 grid) ─────────────────────────────────────────
    panels = [
        ("ai_tech",                "AI / TECH",               "🤖"),
        ("precious_metals",        "PRECIOUS METALS",          "🥇"),
        ("industrial_commodities", "INDUSTRIAL COMMODITIES",   "⚙️"),
        ("macro",                  "MACRO & OTHER",            "🌍"),
    ]
    html += (
        f'<div style="background:#f7f8fc;padding:14px 16px;border-bottom:1px solid #eee">'
        f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:.6px;color:#999;margin-bottom:10px">📊 Sector Dashboard</div>'
        f'<table cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse">'
        # Row 1
        f'<tr>'
        + _sector_card(panels[0][1], panels[0][2], sectors_cfg.get(panels[0][0], {}), sector_insights[panels[0][0]], trends.get(panels[0][0]))
        + _sector_card(panels[1][1], panels[1][2], sectors_cfg.get(panels[1][0], {}), sector_insights[panels[1][0]], trends.get(panels[1][0]))
        + f'</tr>'
        # Row 2
        f'<tr>'
        + _sector_card(panels[2][1], panels[2][2], sectors_cfg.get(panels[2][0], {}), sector_insights[panels[2][0]], trends.get(panels[2][0]))
        + _sector_card(panels[3][1], panels[3][2], sectors_cfg.get(panels[3][0], {}), sector_insights[panels[3][0]], trends.get(panels[3][0]))
        + f'</tr>'
        f'</table></div>'
    )

    # ── Convergence Themes ────────────────────────────────────────────────────
    if themes:
        html += (
            f'<div style="background:#fff;padding:18px 28px;border-bottom:1px solid #eee">'
            f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:.6px;color:#999;margin-bottom:12px">🔁 Convergence Themes</div>'
        )
        for t in themes:
            d  = t.get("direction", "neutral")
            fg, bg = _DIR_COLORS.get(d, ("#555", "#f5f5f5"))
            srcs = ", ".join(t.get("supporting_sources", []))
            strength = t.get("strength", "")
            html += (
                f'<div style="background:{bg};border-left:3px solid {fg};'
                f'padding:9px 13px;margin-bottom:8px;border-radius:0 5px 5px 0">'
                f'<div style="font-size:13px;font-weight:600;color:#222">'
                f'<span style="background:{fg};color:#fff;font-size:9px;font-weight:700;'
                f'padding:2px 6px;border-radius:2px;margin-right:7px;'
                f'letter-spacing:.4px">{d.upper()}</span>{t["theme"]}</div>'
                + (f'<div style="font-size:11px;color:#888;margin-top:3px">'
                   f'{srcs}{(" · " + strength) if strength else ""}</div>' if srcs else "")
                + '</div>'
            )
        html += '</div>'

    # ── Resolved Calls ────────────────────────────────────────────────────────
    if resolved:
        html += (
            f'<div style="background:#fff;padding:18px 28px;border-bottom:1px solid #eee">'
            f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:.6px;color:#999;margin-bottom:12px">🏁 Calls Resolved Today</div>'
        )
        for c in resolved:
            verdict = c.get("status", "")
            v_color = "#1a7a4a" if verdict == "correct" else "#b03030"
            v_icon  = "✅" if verdict == "correct" else "❌"
            chg     = c.get("resolution_change_pct")
            chg_s   = f" ({chg:+.1f}%)" if chg is not None else ""
            html += (
                f'<div style="background:#f8f8f8;border-left:3px solid {v_color};'
                f'padding:10px 14px;margin-bottom:8px;border-radius:0 6px 6px 0">'
                f'<div style="font-size:12px;font-weight:600;color:{v_color};margin-bottom:3px">'
                f'{v_icon} {verdict.upper()}{chg_s}</div>'
                f'<div style="font-size:13px;color:#333">{c["summary"]}</div>'
                f'<div style="font-size:11px;color:#aaa;margin-top:4px">'
                f'📅 Made {c["date_made"]} · ⏱ {c.get("timeframe","?")} · 📰 {c["source"]}'
                f'</div></div>'
            )
        html += '</div>'

    # ── Open Calls Tracker ────────────────────────────────────────────────────
    if open_calls:
        html += (
            f'<div style="background:#fff;padding:18px 28px;border-bottom:1px solid #eee">'
            f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:.6px;color:#999;margin-bottom:12px">📌 Open Calls Tracker</div>'
        )
        for c in open_calls[-10:]:
            insts = ", ".join(c.get("instruments", []))
            d  = c.get("direction", "neutral")
            fg, bg = _DIR_COLORS.get(d, ("#555", "#f5f5f5"))
            html += (
                f'<div style="background:#fffbf0;border:1px solid #f0d878;'
                f'border-radius:6px;padding:10px 14px;margin-bottom:8px">'
                f'<span style="background:{bg};color:{fg};font-size:9px;font-weight:700;'
                f'padding:1px 5px;border-radius:2px;margin-right:6px">{d.upper()}</span>'
                f'<span style="font-size:13px;font-weight:500;color:#333">{c["summary"]}</span>'
                f'<div style="font-size:11px;color:#aaa;margin-top:5px">'
                f'📅 {c["date_made"]}&nbsp;&nbsp;·&nbsp;&nbsp;'
                f'⏱ {c.get("timeframe","?")}&nbsp;&nbsp;·&nbsp;&nbsp;'
                f'📰 {c["source"]}'
                + (f'&nbsp;&nbsp;·&nbsp;&nbsp;🎯 {insts}' if insts else "")
                + '</div></div>'
            )
        html += '</div>'

    # ── Footer ────────────────────────────────────────────────────────────────
    html += (
        f'<div style="text-align:center;padding:14px;font-size:11px;color:#bbb;'
        f'background:#f0f2f5;border-radius:0 0 10px 10px">'
        f'Market Intelligence Monitor &nbsp;·&nbsp; '
        f'{datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}'
        f'</div>'
        f'</div></body></html>'
    )
    return html


# Base CSS shared by the weekly email (daily email uses inline styles instead)
_EMAIL_CSS = """
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:#f0f2f5;margin:0;padding:20px}
.wrap{max-width:700px;margin:0 auto}
.card{background:#fff;border-radius:10px;overflow:hidden;margin-bottom:16px;
      box-shadow:0 1px 3px rgba(0,0,0,.08)}
.hdr{background:#1a1a2e;color:#fff;padding:22px 28px}
.hdr h1{margin:0;font-size:20px;font-weight:600}
.hdr p{margin:4px 0 0;opacity:.65;font-size:12px}
.sec{padding:18px 28px;border-bottom:1px solid #f0f0f0}
.sec:last-child{border-bottom:none}
.sec h2{font-size:11px;text-transform:uppercase;letter-spacing:.6px;color:#999;margin:0 0 12px}
.ins{padding:13px 0;border-bottom:1px solid #f5f5f5}
.ins:last-child{border-bottom:none}
.tag{font-size:11px;padding:2px 8px;border-radius:10px;font-weight:500}
.bullish{background:#e6f9f0;color:#1a7a4a}
.bearish{background:#fef0f0;color:#b03030}
.neutral{background:#eef2ff;color:#3050a0}
.mixed{background:#fffbe6;color:#7a5a00}
.src{background:#f0f0f0;color:#555}
.ins .body{font-size:13px;line-height:1.55;color:#222;margin-bottom:5px}
.ins .reason{font-size:12px;color:#666;line-height:1.4}
.theme{background:#f7f8ff;border-left:3px solid #4a6cf7;
       padding:9px 13px;margin-bottom:9px;border-radius:0 5px 5px 0}
.theme b{font-size:13px}
.theme small{display:block;font-size:11px;color:#999;margin-top:3px}
"""


def format_weekly_email(data: dict, week_label: str) -> str:
    if not data:
        return (
            f"<html><body><p>Weekly synthesis unavailable for {week_label}.</p>"
            f"</body></html>"
        )

    html = (
        f'<!DOCTYPE html><html><head><meta charset="utf-8">'
        f'<style>{_EMAIL_CSS}'
        '.grid{{display:grid;grid-template-columns:1fr 1fr;gap:12px}}'
        '.oc{{background:#f8f9fa;border-radius:6px;padding:14px}}'
        '.oc .asset{{font-weight:600;font-size:12px;margin-bottom:5px;'
        '           text-transform:uppercase;letter-spacing:.4px;color:#555}}'
        '.oc .view{{font-size:13px;color:#333;line-height:1.4}}'
        '.oc .cons{{display:inline-block;margin-bottom:6px}}'
        '</style></head><body><div class="wrap">'
        f'<div class="card"><div class="hdr">'
        f'<h1>📈 Weekly Market Intelligence Report</h1>'
        f'<p>{week_label}</p></div>'
    )

    if data.get("week_summary"):
        html += (
            f'<div class="sec"><h2>🗺 Week in Brief</h2>'
            f'<p style="font-size:14px;line-height:1.65;color:#333;margin:0">'
            f'{data["week_summary"]}</p></div>'
        )

    if data.get("asset_class_outlook"):
        html += '<div class="sec"><h2>📊 Asset Class Outlook</h2><div class="grid">'
        for asset, details in data["asset_class_outlook"].items():
            if isinstance(details, dict):
                cons = details.get("consensus", "")
                view = details.get("summary", "")
            else:
                cons, view = "", str(details)
            label = asset.replace("_", " ").title()
            d_class = cons if cons in ("bullish","bearish","neutral","mixed") else "neutral"
            html += (
                f'<div class="oc"><div class="asset">{label}</div>'
                + (f'<span class="tag {d_class} cons">{cons.upper()}</span>' if cons else "")
                + f'<div class="view">{view}</div></div>'
            )
        html += '</div></div>'

    if data.get("dominant_themes"):
        html += '<div class="sec"><h2>🔁 Dominant Themes This Week</h2>'
        for t in data["dominant_themes"]:
            if isinstance(t, dict):
                freq = t.get("frequency", "")
                d = t.get("direction", "neutral")
                html += (
                    f'<div class="theme">'
                    f'<b><span class="tag {d}" style="margin-right:7px">{d.upper()}</span>'
                    f'{t.get("theme","")}</b>'
                    + (f'<small>{freq}</small>' if freq else "")
                    + '</div>'
                )
            else:
                html += f'<div class="theme"><b>{t}</b></div>'
        html += '</div>'

    if data.get("notable_calls"):
        html += '<div class="sec"><h2>💡 Notable Calls This Week</h2>'
        for c in data["notable_calls"]:
            if isinstance(c, dict):
                src  = c.get("source", "")
                summ = c.get("summary", str(c))
                tf   = c.get("timeframe", "")
                d    = c.get("direction", "neutral")
                conf = c.get("confidence", 0.0)
                html += (
                    f'<div class="ins"><div class="tags">'
                    f'<span class="tag {d}">{d.upper()}</span>'
                    f'<span class="tag src">{src}</span></div>'
                    f'<div class="body">{summ}</div>'
                    + (f'<div class="reason">Timeframe: {tf}</div>' if tf else "")
                    + f'<div class="conf">Confidence {_conf_bar(conf)}</div>'
                    f'</div>'
                )
            else:
                html += f'<div class="ins"><div class="body">{c}</div></div>'
        html += '</div>'

    if data.get("source_divergences"):
        html += '<div class="sec"><h2>⚖️ Notable Divergences</h2>'
        for div in data["source_divergences"]:
            if isinstance(div, dict):
                ca = div.get("camp_a", {})
                cb = div.get("camp_b", {})
                html += (
                    f'<div class="ins">'
                    f'<div class="body"><strong>{div.get("topic","")}</strong></div>'
                    f'<div class="reason">'
                    f'<em>{", ".join(ca.get("sources",[]))}</em>: {ca.get("view","")}<br>'
                    f'<em>{", ".join(cb.get("sources",[]))}</em>: {cb.get("view","")}'
                    f'</div></div>'
                )
        html += '</div>'

    if data.get("key_data_points"):
        html += '<div class="sec"><h2>📐 Key Data Points</h2>'
        for dp in data["key_data_points"]:
            html += f'<div class="ins"><div class="body">• {dp}</div></div>'
        html += '</div>'

    html += (
        '</div>'  # card
        f'<div class="ftr">Market Intelligence Monitor · Weekly · {week_label}</div>'
        '</div></body></html>'
    )
    return html

# ── Convergence Alert ─────────────────────────────────────────────────────────

_ALERTS_FILE = BASE_DIR / "data" / "convergence_alerts.json"


def _load_alerted_dates() -> set[str]:
    if _ALERTS_FILE.exists():
        return set(json.loads(_ALERTS_FILE.read_text()))
    return set()


def _save_alerted_dates(dates: set[str]):
    _ALERTS_FILE.write_text(json.dumps(sorted(dates)))


def _format_alert_email(themes: list[dict], insights: list[dict]) -> str:
    """Compact HTML email for a mid-day convergence alert."""
    W = "max-width:620px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif"
    html = (
        f'<!DOCTYPE html><html><head><meta charset="utf-8"></head>'
        f'<body style="background:#f0f2f5;margin:0;padding:20px">'
        f'<div style="{W}">'
        f'<div style="background:#b03030;border-radius:10px 10px 0 0;padding:18px 24px">'
        f'<div style="color:#fff;font-size:18px;font-weight:700">🚨 Convergence Alert</div>'
        f'<div style="color:rgba(255,255,255,.7);font-size:12px">'
        f'{TODAY.strftime("%A %-d %B %Y")} · Multiple sources converging</div>'
        f'</div>'
        f'<div style="background:#fff;padding:18px 24px;border-bottom:1px solid #eee">'
        f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:.6px;color:#999;margin-bottom:12px">Strong convergence themes</div>'
    )
    for t in themes:
        d  = t.get("direction", "neutral")
        fg, bg = _DIR_COLORS.get(d, ("#555", "#f5f5f5"))
        srcs = ", ".join(t.get("supporting_sources", []))
        html += (
            f'<div style="background:{bg};border-left:3px solid {fg};'
            f'padding:9px 13px;margin-bottom:8px;border-radius:0 5px 5px 0">'
            f'<div style="font-size:13px;font-weight:600;color:#222">'
            f'<span style="background:{fg};color:#fff;font-size:9px;font-weight:700;'
            f'padding:2px 6px;border-radius:2px;margin-right:7px">{d.upper()}</span>'
            f'{t["theme"]}</div>'
            f'<div style="font-size:11px;color:#888;margin-top:3px">{srcs}</div>'
            f'</div>'
        )
    # Top corroborated insights behind these themes
    top = sorted(
        [i for i in insights if i.get("corroboration_confidence", 0) >= 0.5],
        key=lambda x: x.get("corroboration_confidence", 0), reverse=True
    )[:6]
    if top:
        html += (
            '</div><div style="background:#fff;padding:18px 24px">'
            '<div style="font-size:10px;font-weight:700;text-transform:uppercase;'
            'letter-spacing:.6px;color:#999;margin-bottom:12px">Supporting insights</div>'
        )
        for ins in top:
            d  = ins.get("direction", "neutral")
            fg, bg = _DIR_COLORS.get(d, ("#555", "#f5f5f5"))
            html += (
                f'<div style="padding:8px 0;border-bottom:1px solid #f5f5f5">'
                f'<span style="background:{bg};color:{fg};font-size:9px;font-weight:700;'
                f'padding:1px 5px;border-radius:2px;margin-right:6px">{d.upper()}</span>'
                f'<span style="font-size:11px;color:#555">{ins["source"]}</span>'
                f'<div style="font-size:12px;color:#222;margin-top:3px">{ins["summary"]}</div>'
                f'</div>'
            )
    html += (
        f'</div>'
        f'<div style="text-align:center;padding:12px;font-size:11px;color:#bbb">'
        f'Market Intelligence Monitor · Convergence Alert · '
        f'{datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M UTC")}'
        f'</div></div></body></html>'
    )
    return html


def run_convergence_check():
    """Mid-day check: if today's digest has strong convergence not yet alerted, email it."""
    print(f"\n{'='*60}")
    print(f"CONVERGENCE CHECK — {TODAY.isoformat()}")
    print("=" * 60)

    alerted = _load_alerted_dates()
    if TODAY.isoformat() in alerted:
        print("  ✓ Alert already sent today — skipping.")
        return

    fp = DAILY_DIR / f"{TODAY.isoformat()}.json"
    if not fp.exists():
        print("  ✗ No daily JSON found yet — daily run may not have completed.")
        return

    data    = json.loads(fp.read_text())
    themes  = data.get("convergence_themes", [])
    insights= data.get("insights", [])

    # Alert only on strong convergence from ≥ 3 sources
    strong = [
        t for t in themes
        if t.get("strength") == "strong"
        and len(t.get("supporting_sources", [])) >= 3
        and t.get("direction") in ("bullish", "bearish")
    ]

    if not strong:
        print(f"  ✓ No strong convergence today ({len(themes)} theme(s), none qualifying).")
        return

    print(f"  🚨 {len(strong)} strong convergence theme(s) — sending alert…")
    html = _format_alert_email(strong, insights)
    send_email(
        f"🚨 Convergence Alert — {TODAY.strftime('%a %-d %b')} · "
        f"{len(strong)} strong signal(s)",
        html,
    )
    alerted.add(TODAY.isoformat())
    _save_alerted_dates(alerted)


# ── Email Sending ─────────────────────────────────────────────────────────────

def send_email(subject: str, html_body: str):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = YAHOO_EMAIL
    msg["To"]      = RECIPIENT_EMAIL
    msg.attach(MIMEText(html_body, "html"))
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as srv:
            srv.ehlo()
            srv.starttls()
            srv.login(YAHOO_EMAIL, YAHOO_PASSWORD)
            srv.sendmail(YAHOO_EMAIL, RECIPIENT_EMAIL, msg.as_string())
        print(f"  ✅ Email sent: {subject}")
    except Exception as exc:
        print(f"  ❌ Email failed (non-fatal — data still saved): {exc}")

# ── Daily Run ─────────────────────────────────────────────────────────────────

def run_daily():
    print(f"\n{'='*60}")
    print(f"DAILY RUN — {TODAY.isoformat()}")
    print("=" * 60)

    all_items: list[tuple[str, dict]] = []

    # Fetch live prices first — used for snapshot email section and entry-price logging
    print("\n💹 Fetching market snapshot…")
    snapshot = get_market_snapshot()

    # URLs already processed in the previous (FETCH_DAYS_BACK - 1) runs —
    # skip these so each item is only ever extracted once despite the 72h window.
    already_seen = get_already_processed_urls()
    print(f"   ({len(already_seen)} URL(s) already processed in prior runs — will skip)")

    # YouTube
    for source_name, channel_url in YOUTUBE_CHANNELS.items():
        print(f"\n▶  YouTube · {source_name}")
        videos = get_recent_youtube_videos(channel_url, source_name, days_back=FETCH_DAYS_BACK)
        new_videos = [v for v in videos if v["url"] not in already_seen]
        print(f"   {len(new_videos)} new video(s) ({len(videos) - len(new_videos)} already processed)")
        for v in new_videos:
            transcript = get_youtube_transcript(v["id"], v["title"])
            if transcript:
                v["content"] = transcript
                all_items.append(("podcast", v))
            time.sleep(1)

    # RSS
    for source_name, feed_url in RSS_SOURCES.items():
        print(f"\n📰 RSS · {source_name}")
        articles = get_rss_articles(source_name, feed_url, days_back=FETCH_DAYS_BACK)
        new_articles = [a for a in articles if a["url"] not in already_seen]
        print(f"   {len(new_articles)} new article(s) ({len(articles) - len(new_articles)} already processed)")
        for a in new_articles:
            all_items.append(("article", a))

    # X / Twitter (via RSSHub — fails silently if instance is unavailable)
    for source_name, feed_url in X_SOURCES.items():
        print(f"\n🐦 X · {source_name}")
        articles = get_rss_articles(source_name, feed_url, days_back=FETCH_DAYS_BACK)
        new_articles = [a for a in articles if a["url"] not in already_seen]
        print(f"   {len(new_articles)} new post(s) ({len(articles) - len(new_articles)} already processed)")
        for a in new_articles:
            all_items.append(("social", a))

    # Earnings & company news (keyword-filtered)
    for source_name, feed_url in EARNINGS_SOURCES.items():
        print(f"\n📈 Earnings · {source_name}")
        articles = get_rss_articles(source_name, feed_url, days_back=FETCH_DAYS_BACK)
        earnings = [
            a for a in articles
            if any(kw in a["title"].lower() for kw in _EARNINGS_KEYWORDS)
            and a["url"] not in already_seen
        ]
        print(f"   {len(earnings)} earnings item(s)")
        for a in earnings:
            all_items.append(("earnings", a))

    print(f"\n🧠 Extracting insights from {len(all_items)} item(s)…")
    all_insights: list[dict] = []
    for content_type, item in all_items:
        print(f"  → {item['source']}: {item.get('title','')[:65]}")
        insights = extract_insights(item, content_type)
        print(f"     {len(insights)} insight(s)")
        all_insights.extend(insights)
        time.sleep(0.4)

    convergence_themes: list[dict] = []
    if all_insights:
        print(f"\n🔍 Corroborating {len(all_insights)} insight(s)…")
        all_insights, convergence_themes = corroborate_insights(all_insights)

    open_calls    = update_open_calls(all_insights, snapshot)
    resolved      = resolve_expired_calls(snapshot)
    if resolved:
        print(f"\n🏁 {len(resolved)} open call(s) resolved today.")

    print(f"\n📉 Reading sentiment trends…")
    trends = get_sentiment_trends(days=14)

    dashboard: dict = {}
    if all_insights:
        print(f"\n🗺  Synthesising dashboard…")
        dashboard = synthesize_daily_dashboard(all_insights, convergence_themes)

    daily_data = {
        "date":               TODAY.isoformat(),
        "sources_processed":  sorted({i["source"] for i in all_insights}),
        "insights":           all_insights,
        "convergence_themes": convergence_themes,
        "open_calls":         open_calls,
        "resolved_calls":     resolved,
        "dashboard":          dashboard,
        "snapshot":           snapshot,
        "sentiment_trends":   trends,
    }

    out = DAILY_DIR / f"{TODAY.isoformat()}.json"
    out.write_text(json.dumps(daily_data, indent=2))
    print(f"\n💾 Saved → {out}")

    if not all_insights:
        subject  = f"📊 Market Digest — {TODAY.strftime('%a %-d %b')} · No new content today"
        html     = (
            "<html><body style='font-family:sans-serif;padding:24px'>"
            f"<h2>Market Digest — {TODAY.isoformat()}</h2>"
            "<p>No new content was published today by monitored sources.</p>"
            "</body></html>"
        )
    else:
        subject = (
            f"📊 Market Digest — {TODAY.strftime('%a %-d %b')} · "
            f"{len(all_insights)} insights · "
            f"{len(convergence_themes)} convergence theme(s)"
        )
        html = format_daily_email(daily_data)

    send_email(subject, html)
    return daily_data

# ── Weekly Run ────────────────────────────────────────────────────────────────

def run_weekly():
    print(f"\n{'='*60}")
    print(f"WEEKLY SYNTHESIS — {TODAY.isoformat()}")
    print("=" * 60)

    weekly_data = generate_weekly_synthesis()
    week_label  = TODAY.strftime("W%V %Y")

    out = WEEKLY_DIR / f"{TODAY.strftime('%Y-W%V')}.json"
    out.write_text(json.dumps(weekly_data, indent=2))
    print(f"💾 Saved → {out}")

    subject = f"📈 Weekly Market Intelligence Report — {week_label}"
    send_email(subject, format_weekly_email(weekly_data, week_label))

# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    if "--convergence-check" in sys.argv:
        run_convergence_check()
    else:
        run_daily()
        if IS_WEEKLY:
            run_weekly()
