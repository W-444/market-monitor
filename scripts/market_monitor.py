#!/usr/bin/env python3
"""
Market Intelligence Monitor
============================
Daily: fetches last 24h of content from financial analysts, extracts insights
       via Claude, corroborates across sources, emails a digest.
Weekly (Sundays): synthesises the past 7 daily JSON files into a weekly report.

Sources monitored
-----------------
YouTube : Thoughtful Money, Eurodollar University, All-In Podcast
RSS     : Lyn Alden, Doomberg, SemiAnalysis, Macro Voices, Kitco News,
          Real Investment Advice (Lance Roberts), Sprott, Grant Williams
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

import feedparser
import anthropic
from youtube_transcript_api import YouTubeTranscriptApi

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
MAX_ARTICLE_CHARS    = 8000

# ── Sources ───────────────────────────────────────────────────────────────────

YOUTUBE_CHANNELS = {
    "Thoughtful Money":      "https://www.youtube.com/@thoughtfulmoney",
    "Eurodollar University": "https://www.youtube.com/@EurodollarUniversity",
    "All-In Podcast":        "https://www.youtube.com/@allin",
}

RSS_SOURCES = {
    "Lyn Alden":              "https://www.lynalden.com/feed/",
    "Doomberg":               "https://doomberg.substack.com/feed",
    "SemiAnalysis":           "https://semianalysis.substack.com/feed",
    "Macro Voices":           "https://www.macrovoices.com/feed",
    "Kitco News":             "https://www.kitco.com/rss/kitco-news.rss",
    "Real Investment Advice": "https://realinvestmentadvice.com/feed/",
    "Sprott":                 "https://sprott.com/feed/",
    # Grant Williams' feed URL – update if needed
    "Grant Williams":         "https://www.ttmygh.com/feed/",
}

# Luke Gromen (FFTT) posts primarily behind a paid newsletter and on X.
# Add his RSS here if you subscribe: e.g. "Luke Gromen": "https://fftt-llc.com/feed/"

# ── Content Fetching ──────────────────────────────────────────────────────────

def get_recent_youtube_videos(channel_url: str, source_name: str,
                               days_back: int = 1) -> list[dict]:
    """Return video metadata for videos published in the last `days_back` days."""
    cutoff = (TODAY - datetime.timedelta(days=days_back)).strftime("%Y%m%d")
    try:
        result = subprocess.run(
            [
                "yt-dlp", "--flat-playlist", "--playlist-end", "8",
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
            if upload_date and upload_date >= cutoff:
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
    """Fetch and return (truncated) transcript text, or None if unavailable.

    Handles both the legacy classmethod API (< 0.6.x) and the newer
    instance-based API (>= 0.6.x) so the script works across library versions.
    """
    try:
        # Newer API (youtube-transcript-api >= 0.6.x): instantiate first
        try:
            api = YouTubeTranscriptApi()
            entries = api.fetch(video_id, languages=["en"])
            text = " ".join(e.text for e in entries)
        except AttributeError:
            # Fallback: older classmethod API (< 0.6.x)
            entries = YouTubeTranscriptApi.get_transcript(video_id, languages=["en"])
            text = " ".join(e["text"] for e in entries)
        return text[:MAX_TRANSCRIPT_CHARS]
    except Exception as exc:
        msg = str(exc).lower()
        if any(k in msg for k in ("disabled", "no transcript", "notranscriptfound", "could not retrieve")):
            print(f"    No transcript available: {title[:60]}")
        else:
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

# ── Open Calls Tracker ────────────────────────────────────────────────────────

def _load_open_calls() -> list[dict]:
    if CALLS_FILE.exists():
        return json.loads(CALLS_FILE.read_text())
    return []


def _save_open_calls(calls: list[dict]):
    CALLS_FILE.write_text(json.dumps(calls, indent=2))


def update_open_calls(new_insights: list[dict]) -> list[dict]:
    """Log high-specificity insights with timeframes; return all open calls."""
    calls = _load_open_calls()
    existing = {c["summary"] for c in calls}

    for ins in new_insights:
        if (
            ins.get("specificity") == "high"
            and ins.get("timeframe")
            and ins["summary"] not in existing
        ):
            calls.append({
                "summary":           ins["summary"],
                "source":            ins["source"],
                "source_url":        ins.get("source_url", ""),
                "direction":         ins["direction"],
                "instruments":       ins.get("instruments", []),
                "timeframe":         ins["timeframe"],
                "date_made":         ins["content_date"],
                "source_confidence": ins.get("source_confidence", 0.0),
                "status":            "open",
            })
            existing.add(ins["summary"])

    _save_open_calls(calls)
    return [c for c in calls if c["status"] == "open"]

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

def _conf_bar(score: float) -> str:
    filled = round(min(max(score, 0), 1) * 10)
    return "█" * filled + "░" * (10 - filled) + f"  {score:.0%}"


_EMAIL_CSS = """
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:#f0f2f5;margin:0;padding:20px}
.wrap{max-width:680px;margin:0 auto}
.card{background:#fff;border-radius:10px;overflow:hidden;margin-bottom:16px;
      box-shadow:0 1px 3px rgba(0,0,0,.08)}
.hdr{background:#1a1a2e;color:#fff;padding:22px 28px}
.hdr h1{margin:0;font-size:20px;font-weight:600}
.hdr p{margin:4px 0 0;opacity:.65;font-size:12px}
.sec{padding:18px 28px;border-bottom:1px solid #f0f0f0}
.sec:last-child{border-bottom:none}
.sec h2{font-size:11px;text-transform:uppercase;letter-spacing:.6px;
         color:#999;margin:0 0 12px}
.theme{background:#f7f8ff;border-left:3px solid #4a6cf7;
       padding:9px 13px;margin-bottom:9px;border-radius:0 5px 5px 0}
.theme b{font-size:13px}
.theme small{display:block;font-size:11px;color:#999;margin-top:3px}
.ins{padding:13px 0;border-bottom:1px solid #f5f5f5}
.ins:last-child{border-bottom:none}
.tags{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:7px}
.tag{font-size:11px;padding:2px 8px;border-radius:10px;font-weight:500}
.bullish{background:#e6f9f0;color:#1a7a4a}
.bearish{background:#fef0f0;color:#b03030}
.neutral{background:#eef2ff;color:#3050a0}
.mixed{background:#fffbe6;color:#7a5a00}
.src{background:#f0f0f0;color:#555}
.ins .body{font-size:13px;line-height:1.55;color:#222;margin-bottom:5px}
.ins .reason{font-size:12px;color:#666;line-height:1.4}
.ins .conf{font-size:11px;color:#aaa;font-family:monospace;margin-top:6px;
           line-height:1.8}
.call{background:#fffbf0;border:1px solid #f0d878;border-radius:6px;
      padding:10px 14px;margin-bottom:8px}
.call .cs{font-size:13px;font-weight:500;color:#333}
.call .cm{font-size:11px;color:#999;margin-top:4px}
.ftr{text-align:center;padding:14px;font-size:11px;color:#bbb}
"""


def format_daily_email(data: dict) -> str:
    date_str  = data["date"]
    insights  = data.get("insights", [])
    themes    = data.get("convergence_themes", [])
    open_calls = data.get("open_calls", [])
    src_count = len(set(i["source"] for i in insights))

    html = (
        f'<!DOCTYPE html><html><head><meta charset="utf-8">'
        f'<style>{_EMAIL_CSS}</style></head>'
        f'<body><div class="wrap">'
        f'<div class="card"><div class="hdr">'
        f'<h1>📊 Market Intelligence Digest</h1>'
        f'<p>{date_str} · {len(insights)} insights · {src_count} sources</p>'
        f'</div>'
    )

    # Convergence themes
    if themes:
        html += '<div class="sec"><h2>🔁 Convergence Themes</h2>'
        for t in themes:
            d = t.get("direction", "neutral")
            srcs = ", ".join(t.get("supporting_sources", []))
            strength = t.get("strength", "")
            html += (
                f'<div class="theme">'
                f'<b><span class="tag {d}" style="margin-right:7px">{d.upper()}</span>'
                f'{t["theme"]}</b>'
                f'<small>{srcs}{" · " + strength if strength else ""}</small>'
                f'</div>'
            )
        html += '</div>'

    # Key insights (sorted by combined confidence)
    sorted_ins = sorted(
        insights,
        key=lambda x: x.get("source_confidence", 0) + x.get("corroboration_confidence", 0),
        reverse=True,
    )
    html += '<div class="sec"><h2>💡 Key Insights</h2>'
    for ins in sorted_ins[:18]:
        d    = ins.get("direction", "neutral")
        inst = ", ".join(ins.get("instruments", []))
        tf   = ins.get("timeframe", "")
        nd   = ins.get("notable_data", "")
        sc   = ins.get("source_confidence", 0.0)
        cc   = ins.get("corroboration_confidence", 0.0)
        cby  = ", ".join(ins.get("corroborated_by", []))

        html += (
            f'<div class="ins"><div class="tags">'
            f'<span class="tag {d}">{d.upper()}</span>'
            f'<span class="tag src">{ins["source"]}</span>'
            + (f'<span class="tag src">{inst}</span>' if inst else "")
            + f'<span style="font-size:11px;color:#ccc;margin-left:auto">{ins.get("content_date","")}</span>'
            f'</div>'
            f'<div class="body">{ins["summary"]}</div>'
            f'<div class="reason">{ins.get("key_reasoning","")}'
            + (f' <em>Data: {nd}</em>' if nd else "")
            + f'</div>'
            f'<div class="conf">'
            f'Source confidence&nbsp;&nbsp;&nbsp;{_conf_bar(sc)}<br>'
            f'Corroboration&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{_conf_bar(cc)}'
            + (f'<br>Corroborated by: {cby}' if cby else "")
            + (f'<br>Timeframe: {tf}' if tf else "")
            + '</div></div>'
        )
    html += '</div>'

    # Open calls
    if open_calls:
        html += '<div class="sec"><h2>📌 Open Calls Tracker</h2>'
        for c in open_calls[-12:]:
            insts = ", ".join(c.get("instruments", []))
            html += (
                f'<div class="call"><div class="cs">{c["summary"]}</div>'
                f'<div class="cm">📅 {c["date_made"]} · ⏱ {c.get("timeframe","?")} · '
                f'📰 {c["source"]}'
                + (f' · 🎯 {insts}' if insts else "")
                + f'</div></div>'
            )
        html += '</div>'

    html += (
        f'</div>'  # card
        f'<div class="ftr">Market Intelligence Monitor · '
        f'{datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}</div>'
        f'</div></body></html>'
    )
    return html


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

# ── Email Sending ─────────────────────────────────────────────────────────────

def send_email(subject: str, html_body: str):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = YAHOO_EMAIL
    msg["To"]      = RECIPIENT_EMAIL
    msg.attach(MIMEText(html_body, "html"))
    try:
        with smtplib.SMTP("smtp.mail.yahoo.com", 587) as srv:
            srv.ehlo()
            srv.starttls()
            srv.login(YAHOO_EMAIL, YAHOO_PASSWORD)
            srv.sendmail(YAHOO_EMAIL, RECIPIENT_EMAIL, msg.as_string())
        print(f"  ✅ Email sent: {subject}")
    except Exception as exc:
        print(f"  ❌ Email failed: {exc}")
        raise

# ── Daily Run ─────────────────────────────────────────────────────────────────

def run_daily():
    print(f"\n{'='*60}")
    print(f"DAILY RUN — {TODAY.isoformat()}")
    print("=" * 60)

    all_items: list[tuple[str, dict]] = []

    # YouTube
    for source_name, channel_url in YOUTUBE_CHANNELS.items():
        print(f"\n▶  YouTube · {source_name}")
        videos = get_recent_youtube_videos(channel_url, source_name, days_back=1)
        print(f"   {len(videos)} new video(s)")
        for v in videos:
            transcript = get_youtube_transcript(v["id"], v["title"])
            if transcript:
                v["content"] = transcript
                all_items.append(("podcast", v))
            time.sleep(1)

    # RSS
    for source_name, feed_url in RSS_SOURCES.items():
        print(f"\n📰 RSS · {source_name}")
        articles = get_rss_articles(source_name, feed_url, days_back=1)
        print(f"   {len(articles)} new article(s)")
        for a in articles:
            all_items.append(("article", a))

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

    open_calls = update_open_calls(all_insights)

    daily_data = {
        "date":              TODAY.isoformat(),
        "sources_processed": sorted({i["source"] for i in all_insights}),
        "insights":          all_insights,
        "convergence_themes": convergence_themes,
        "open_calls":        open_calls,
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
    run_daily()
    if IS_WEEKLY:
        run_weekly()
