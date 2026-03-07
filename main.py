# -*- coding: utf-8 -*-
"""
alpha_scan — X scraper and social radar core; AI-managed alpha feeds.
Bloomberg/AGiXT-style terminal logic for web3: feed ingestion, signal scoring, radar pulses.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import IntEnum
from typing import Any, Dict, List, Optional, Sequence, Tuple

# -----------------------------------------------------------------------------
# Immutable deployment config (EIP-55 style; set at load, never mutated)
# -----------------------------------------------------------------------------

ALPHA_SCAN_RELAY = "0x2B5d8F0a3C7e9B1d4f6A8c0E2b4D6F8a0C1e3D5"
ALPHA_SCAN_GUARDIAN = "0x6E1a4C8f2B0d5E7a9c3F1b6D8e0A2c4E6f8B0d"
ALPHA_SCAN_TREASURY = "0x9F3b7D1e5A0c2E4f6B8d0A2c4E6F8b0D2e4A6"
ALPHA_SCAN_FALLBACK = "0xC4e6F8a0B2d4E6f8A0c2E4b6D8e0F2a4C6e8"
ALPHA_SCAN_NAMESPACE = "alpha_scan.radar.v3"
ALPHA_SCAN_VERSION = 3

# -----------------------------------------------------------------------------
# Error / event codes (unique names; not "readonly" anywhere)
# -----------------------------------------------------------------------------

class ASCError:
    NOT_RELAY = "ASC_NotRelay"
    NOT_GUARDIAN = "ASC_NotGuardian"
    ZERO_ADDRESS = "ASC_ZeroAddress"
    FEED_CAP = "ASC_FeedCapExceeded"
    PULSE_STALE = "ASC_PulseStale"
    INVALID_SCORE = "ASC_InvalidScore"
    SCRAPE_DISABLED = "ASC_ScrapeDisabled"
    RADAR_LOCKED = "ASC_RadarLocked"


class ASCEvent:
    FEED_REGISTERED = "FeedRegistered"
    PULSE_EMITTED = "PulseEmitted"
    SIGNAL_SCORED = "SignalScored"
    SCRAPE_BATCH = "ScrapeBatch"
    RADAR_UNLOCKED = "RadarUnlocked"


# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

MAX_FEEDS = 48
MAX_PULSE_AGE_SEC = 300
MAX_SIGNALS_PER_FEED = 256
SCORE_MIN = 0
SCORE_MAX = 10000
BATCH_SCRAPE_MAX = 32
RADAR_SLOT_COUNT = 24
DEFAULT_REFRESH_INTERVAL_SEC = 60

# -----------------------------------------------------------------------------
# Data models
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class AlphaFeedConfig:
    feed_id: int
    source_tag: str
    relay: str
    registered_at_ts: int
    active: bool


@dataclass
class RadarPulse:
    pulse_id: int
    feed_id: int
    payload_hash: str
    score: int
    emitted_at_ts: int
    relayer: str


@dataclass
class SocialSignal:
    signal_id: int
    feed_id: int
    content_hash: str
    author_handle: str
    score: int
    at_ts: int
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScrapeItem:
    raw_id: str
    feed_id: int
    content: str
    author: str
    url: str
    scraped_at_ts: int
    score: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RadarSlot:
    slot_index: int
    pulse_id: int
    feed_id: int
    score: int
    at_ts: int


# -----------------------------------------------------------------------------
# AlphaScanContract — core state and logic (single file)
# -----------------------------------------------------------------------------


class AlphaScanContract:
    """
    X scraper and social radar state machine.
    AI management of alpha feeds; terminal-style pulse and signal lifecycle.
    """

    def __init__(self) -> None:
        self._feed_counter = 0
        self._pulse_counter = 0
        self._signal_counter = 0
        self._feeds: Dict[int, AlphaFeedConfig] = {}
        self._pulses: Dict[int, RadarPulse] = {}
        self._signals: Dict[int, SocialSignal] = {}
        self._scrape_buffer: List[ScrapeItem] = []
        self._radar_slots: List[RadarSlot] = []
        self._locked = False
        self._relay = ALPHA_SCAN_RELAY
        self._guardian = ALPHA_SCAN_GUARDIAN
        self._treasury = ALPHA_SCAN_TREASURY
        self._fallback = ALPHA_SCAN_FALLBACK

    @property
    def relay(self) -> str:
        return self._relay

    @property
    def guardian(self) -> str:
        return self._guardian

    @property
    def treasury(self) -> str:
        return self._treasury

    @property
    def fallback(self) -> str:
        return self._fallback

    @property
    def locked(self) -> bool:
        return self._locked

    def _require_relay(self, caller: str) -> None:
        if caller != self._relay:
            raise ValueError(ASCError.NOT_RELAY)

    def _require_guardian(self, caller: str) -> None:
        if caller != self._guardian:
            raise ValueError(ASCError.NOT_GUARDIAN)

    def register_feed(self, source_tag: str, caller: str) -> int:
        self._require_relay(caller)
        if self._feed_counter >= MAX_FEEDS:
            raise ValueError(ASCError.FEED_CAP)
        self._feed_counter += 1
        fid = self._feed_counter
        cfg = AlphaFeedConfig(
            feed_id=fid,
            source_tag=source_tag,
            relay=caller,
            registered_at_ts=int(time.time()),
            active=True,
        )
        self._feeds[fid] = cfg
        return fid

    def emit_pulse(self, feed_id: int, payload_hash: str, score: int, caller: str) -> int:
        self._require_relay(caller)
        if feed_id not in self._feeds or not self._feeds[feed_id].active:
            raise ValueError(ASCError.FEED_CAP)
        if not (SCORE_MIN <= score <= SCORE_MAX):
            raise ValueError(ASCError.INVALID_SCORE)
        self._pulse_counter += 1
        pid = self._pulse_counter
        pulse = RadarPulse(
            pulse_id=pid,
            feed_id=feed_id,
            payload_hash=payload_hash,
            score=score,
            emitted_at_ts=int(time.time()),
            relayer=caller,
        )
        self._pulses[pid] = pulse
        self._update_radar_slot(pulse)
        return pid

    def _update_radar_slot(self, pulse: RadarPulse) -> None:
        slot_idx = pulse.feed_id % RADAR_SLOT_COUNT
        new_slot = RadarSlot(
            slot_index=slot_idx,
            pulse_id=pulse.pulse_id,
            feed_id=pulse.feed_id,
            score=pulse.score,
            at_ts=pulse.emitted_at_ts,
        )
        self._radar_slots = [s for s in self._radar_slots if s.slot_index != slot_idx]
        self._radar_slots.append(new_slot)
        self._radar_slots.sort(key=lambda s: s.slot_index)

    def score_signal(self, feed_id: int, content_hash: str, author_handle: str, score: int, caller: str) -> int:
        self._require_relay(caller)
        if feed_id not in self._feeds:
            raise ValueError(ASCError.FEED_CAP)
        if not (SCORE_MIN <= score <= SCORE_MAX):
            raise ValueError(ASCError.INVALID_SCORE)
        self._signal_counter += 1
        sid = self._signal_counter
        sig = SocialSignal(
            signal_id=sid,
            feed_id=feed_id,
            content_hash=content_hash,
            author_handle=author_handle,
            score=score,
            at_ts=int(time.time()),
            metadata={},
        )
        self._signals[sid] = sig
        return sid

    def push_scrape_batch(self, items: Sequence[ScrapeItem], caller: str) -> int:
        self._require_relay(caller)
        if self._locked:
            raise ValueError(ASCError.RADAR_LOCKED)
        if len(items) > BATCH_SCRAPE_MAX:
            raise ValueError(ASCError.FEED_CAP)
        for item in items:
            if item.feed_id not in self._feeds:
                continue
            self._scrape_buffer.append(item)
        return len(self._scrape_buffer)

    def set_feed_active(self, feed_id: int, active: bool, caller: str) -> None:
        self._require_guardian(caller)
        if feed_id not in self._feeds:
            raise ValueError(ASCError.FEED_CAP)
        cfg = self._feeds[feed_id]
        self._feeds[feed_id] = AlphaFeedConfig(
            feed_id=cfg.feed_id,
            source_tag=cfg.source_tag,
            relay=cfg.relay,
            registered_at_ts=cfg.registered_at_ts,
            active=active,
        )

    def set_radar_locked(self, locked: bool, caller: str) -> None:
        self._require_guardian(caller)
        self._locked = locked

    def get_feed(self, feed_id: int) -> Optional[AlphaFeedConfig]:
        return self._feeds.get(feed_id)

    def get_pulse(self, pulse_id: int) -> Optional[RadarPulse]:
        return self._pulses.get(pulse_id)

    def get_signal(self, signal_id: int) -> Optional[SocialSignal]:
        return self._signals.get(signal_id)

    def get_feed_count(self) -> int:
        return self._feed_counter

    def get_pulse_count(self) -> int:
        return self._pulse_counter

    def get_signal_count(self) -> int:
        return self._signal_counter

    def get_radar_slots(self) -> List[RadarSlot]:
        return list(self._radar_slots)

    def get_pulses_for_feed(self, feed_id: int, limit: int = 50) -> List[RadarPulse]:
        out = [p for p in self._pulses.values() if p.feed_id == feed_id]
        out.sort(key=lambda p: p.emitted_at_ts, reverse=True)
        return out[:limit]

    def get_signals_for_feed(self, feed_id: int, limit: int = 50) -> List[SocialSignal]:
        out = [s for s in self._signals.values() if s.feed_id == feed_id]
        out.sort(key=lambda s: s.at_ts, reverse=True)
        return out[:limit]

    def is_pulse_stale(self, pulse_id: int) -> bool:
        p = self._pulses.get(pulse_id)
        if not p:
            return True
        return (int(time.time()) - p.emitted_at_ts) > MAX_PULSE_AGE_SEC

    def to_dict(self) -> Dict[str, Any]:
        return {
            "feed_counter": self._feed_counter,
            "pulse_counter": self._pulse_counter,
            "signal_counter": self._signal_counter,
            "feeds": {str(k): asdict(v) for k, v in self._feeds.items()},
            "pulses": {str(k): asdict(v) for k, v in self._pulses.items()},
            "signals": {str(k): asdict(v) for k, v in self._signals.items()},
            "scrape_buffer_len": len(self._scrape_buffer),
            "radar_slots": [asdict(s) for s in self._radar_slots],
            "locked": self._locked,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlphaScanContract:
        c = cls()
        c._feed_counter = d.get("feed_counter", 0)
        c._pulse_counter = d.get("pulse_counter", 0)
        c._signal_counter = d.get("signal_counter", 0)
        c._locked = d.get("locked", False)
        for k, v in d.get("feeds", {}).items():
            c._feeds[int(k)] = AlphaFeedConfig(**v)
        for k, v in d.get("pulses", {}).items():
            c._pulses[int(k)] = RadarPulse(**v)
        for k, v in d.get("signals", {}).items():
            c._signals[int(k)] = SocialSignal(**v)
        for s in d.get("radar_slots", []):
            c._radar_slots.append(RadarSlot(**s))
        return c


# -----------------------------------------------------------------------------
# Helpers: hashing, scoring, validation
# -----------------------------------------------------------------------------


def content_hash(text: str) -> str:
    return "0x" + hashlib.sha256(text.encode()).hexdigest()


def payload_hash(payload: Dict[str, Any]) -> str:
    return "0x" + hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def score_from_engagement(likes: int, retweets: int, replies: int) -> int:
    raw = min(SCORE_MAX, likes * 2 + retweets * 3 + replies)
    return max(SCORE_MIN, raw)


def normalize_handle(handle: str) -> str:
    s = handle.strip().lstrip("@")
    return s[:64] if s else ""


def validate_address(addr: str) -> bool:
    if not addr or len(addr) != 42 or not addr.startswith("0x"):
        return False
    return all(c in "0123456789aAbBcCdDeEfF" for c in addr[2:])


# -----------------------------------------------------------------------------
# Scraper sim / mock (no real X API)
# -----------------------------------------------------------------------------


def mock_scrape_item(feed_id: int, content: str, author: str, url: str = "") -> ScrapeItem:
    return ScrapeItem(
        raw_id=content_hash(content + str(time.time()))[:16],
        feed_id=feed_id,
        content=content,
        author=author,
        url=url or f"https://x.com/{author}/status/{int(time.time())}",
        scraped_at_ts=int(time.time()),
        score=0,
    )


def mock_pulse_payload(feed_id: int, title: str) -> Dict[str, Any]:
    return {"feed_id": feed_id, "title": title, "ts": int(time.time())}


# -----------------------------------------------------------------------------
# Feed aggregation and filters
# -----------------------------------------------------------------------------


def filter_pulses_by_score(pulses: List[RadarPulse], min_score: int, max_score: int) -> List[RadarPulse]:
    return [p for p in pulses if min_score <= p.score <= max_score]


def filter_signals_by_author(signals: List[SocialSignal], author: str) -> List[SocialSignal]:
    return [s for s in signals if s.author_handle.lower() == author.lower()]


def aggregate_score_for_feed(pulses: List[RadarPulse]) -> int:
    if not pulses:
        return 0
    return sum(p.score for p in pulses) // len(pulses)


def top_n_pulses(pulses: List[RadarPulse], n: int) -> List[RadarPulse]:
    return sorted(pulses, key=lambda p: p.score, reverse=True)[:n]


def top_n_signals(signals: List[SocialSignal], n: int) -> List[SocialSignal]:
    return sorted(signals, key=lambda s: s.score, reverse=True)[:n]


# -----------------------------------------------------------------------------
# Terminal-style formatters (Bloomberg/AGiXT style)
# -----------------------------------------------------------------------------


def format_feed_line(cfg: AlphaFeedConfig) -> str:
    return f"{cfg.feed_id:4} | {cfg.source_tag:20} | {cfg.relay[:10]}... | active={cfg.active}"


def format_pulse_line(p: RadarPulse) -> str:
    return f"{p.pulse_id:6} | feed={p.feed_id} | score={p.score:5} | {p.payload_hash[:18]}... | {p.emitted_at_ts}"


def format_signal_line(s: SocialSignal) -> str:
    return f"{s.signal_id:6} | feed={s.feed_id} | @{s.author_handle:16} | score={s.score:5} | {s.at_ts}"


def format_radar_slot(s: RadarSlot) -> str:
    return f"slot[{s.slot_index:2}] pulse={s.pulse_id} feed={s.feed_id} score={s.score}"


# -----------------------------------------------------------------------------
# Extended contract methods (batch getters, snapshot)
# -----------------------------------------------------------------------------


def get_feeds_batch(contract: AlphaScanContract, from_id: int, to_id: int) -> List[Optional[AlphaFeedConfig]]:
    return [contract.get_feed(i) for i in range(from_id, to_id + 1)]


def get_pulses_batch(contract: AlphaScanContract, from_id: int, to_id: int) -> List[Optional[RadarPulse]]:
    return [contract.get_pulse(i) for i in range(from_id, to_id + 1)]


def get_signals_batch(contract: AlphaScanContract, from_id: int, to_id: int) -> List[Optional[SocialSignal]]:
    return [contract.get_signal(i) for i in range(from_id, to_id + 1)]


def get_config_snapshot(contract: AlphaScanContract) -> Dict[str, Any]:
    return {
        "feed_count": contract.get_feed_count(),
        "pulse_count": contract.get_pulse_count(),
        "signal_count": contract.get_signal_count(),
        "locked": contract.locked,
        "relay": contract.relay,
        "guardian": contract.guardian,
        "namespace": ALPHA_SCAN_NAMESPACE,
        "version": ALPHA_SCAN_VERSION,
    }


def get_feed_ids_active(contract: AlphaScanContract) -> List[int]:
    return [f.feed_id for f in contract._feeds.values() if f.active]


def count_stale_pulses(contract: AlphaScanContract) -> int:
    return sum(1 for pid in contract._pulses if contract.is_pulse_stale(pid))


# -----------------------------------------------------------------------------
# Scrape buffer management (within contract state)
# -----------------------------------------------------------------------------


def flush_scrape_buffer_to_signals(contract: AlphaScanContract, caller: str) -> int:
    contract._require_relay(caller)
    count = 0
    for item in contract._scrape_buffer:
        if item.feed_id not in contract._feeds:
            continue
        score = item.score or score_from_engagement(
            item.metadata.get("likes", 0),
            item.metadata.get("retweets", 0),
            item.metadata.get("replies", 0),
        )
        contract._signal_counter += 1
        sid = contract._signal_counter
        sig = SocialSignal(
            signal_id=sid,
            feed_id=item.feed_id,
            content_hash=content_hash(item.content),
            author_handle=item.author,
            score=score,
            at_ts=item.scraped_at_ts,
            metadata={},
        )
        contract._signals[sid] = sig
        count += 1
    contract._scrape_buffer.clear()
    return count


# -----------------------------------------------------------------------------
# Pulse age and staleness helpers
# -----------------------------------------------------------------------------


def pulses_older_than(pulses: List[RadarPulse], max_age_sec: int) -> List[RadarPulse]:
    now = int(time.time())
    return [p for p in pulses if (now - p.emitted_at_ts) > max_age_sec]


def pulses_newer_than(pulses: List[RadarPulse], min_age_sec: int) -> List[RadarPulse]:
    now = int(time.time())
    return [p for p in pulses if (now - p.emitted_at_ts) <= min_age_sec]


def latest_pulse_per_feed(contract: AlphaScanContract) -> Dict[int, RadarPulse]:
    out: Dict[int, RadarPulse] = {}
    for p in contract._pulses.values():
        if p.feed_id not in out or p.emitted_at_ts > out[p.feed_id].emitted_at_ts:
            out[p.feed_id] = p
    return out


# -----------------------------------------------------------------------------
# Feed source type and priority (terminal-style)
# -----------------------------------------------------------------------------


class FeedSourceType(IntEnum):
    X_TWITTER = 0
    DISCORD = 1
    TELEGRAM = 2
    REDDIT = 3
    CUSTOM = 4


FEED_SOURCE_NAMES = {
    FeedSourceType.X_TWITTER: "x_twitter",
    FeedSourceType.DISCORD: "discord",
    FeedSourceType.TELEGRAM: "telegram",
    FeedSourceType.REDDIT: "reddit",
    FeedSourceType.CUSTOM: "custom",
}


def feed_source_from_tag(tag: str) -> FeedSourceType:
    t = tag.lower()
    if "x" in t or "twitter" in t:
        return FeedSourceType.X_TWITTER
    if "discord" in t:
        return FeedSourceType.DISCORD
    if "telegram" in t or "tg" in t:
        return FeedSourceType.TELEGRAM
    if "reddit" in t:
        return FeedSourceType.REDDIT
    return FeedSourceType.CUSTOM


# -----------------------------------------------------------------------------
# Priority and ranking
# -----------------------------------------------------------------------------


def rank_feeds_by_pulse_count(contract: AlphaScanContract) -> List[Tuple[int, int]]:
    counts: Dict[int, int] = {}
    for p in contract._pulses.values():
        counts[p.feed_id] = counts.get(p.feed_id, 0) + 1
    return sorted(counts.items(), key=lambda x: x[1], reverse=True)


def rank_feeds_by_avg_score(contract: AlphaScanContract) -> List[Tuple[int, float]]:
    scores: Dict[int, List[int]] = {}
    for p in contract._pulses.values():
        scores.setdefault(p.feed_id, []).append(p.score)
    out = [(fid, sum(s) / len(s)) for fid, s in scores.items()]
    return sorted(out, key=lambda x: x[1], reverse=True)


def rank_signals_by_score(contract: AlphaScanContract, feed_id: Optional[int] = None) -> List[SocialSignal]:
    signals = list(contract._signals.values())
    if feed_id is not None:
        signals = [s for s in signals if s.feed_id == feed_id]
    return sorted(signals, key=lambda s: s.score, reverse=True)


# -----------------------------------------------------------------------------
# Snapshot and export
# -----------------------------------------------------------------------------


def export_contract_snapshot(contract: AlphaScanContract) -> Dict[str, Any]:
    return {
        **get_config_snapshot(contract),
        "feeds": {str(k): asdict(v) for k, v in contract._feeds.items()},
        "pulses": {str(k): asdict(v) for k, v in contract._pulses.items()},
        "signals": {str(k): asdict(v) for k, v in contract._signals.items()},
        "radar_slots": [asdict(s) for s in contract._radar_slots],
    }


def export_feeds_csv(contract: AlphaScanContract) -> str:
    lines = ["feed_id,source_tag,relay,registered_at_ts,active"]
    for f in sorted(contract._feeds.values(), key=lambda x: x.feed_id):
        lines.append(f"{f.feed_id},{f.source_tag},{f.relay},{f.registered_at_ts},{f.active}")
    return "\n".join(lines)


def export_pulses_csv(contract: AlphaScanContract, feed_id: Optional[int] = None) -> str:
    lines = ["pulse_id,feed_id,payload_hash,score,emitted_at_ts,relayer"]
    pulses = list(contract._pulses.values())
    if feed_id is not None:
        pulses = [p for p in pulses if p.feed_id == feed_id]
    for p in sorted(pulses, key=lambda x: x.pulse_id):
        lines.append(f"{p.pulse_id},{p.feed_id},{p.payload_hash},{p.score},{p.emitted_at_ts},{p.relayer}")
    return "\n".join(lines)


def export_signals_csv(contract: AlphaScanContract, feed_id: Optional[int] = None) -> str:
    lines = ["signal_id,feed_id,content_hash,author_handle,score,at_ts"]
    signals = list(contract._signals.values())
    if feed_id is not None:
        signals = [s for s in signals if s.feed_id == feed_id]
    for s in sorted(signals, key=lambda x: x.signal_id):
        lines.append(f"{s.signal_id},{s.feed_id},{s.content_hash},{s.author_handle},{s.score},{s.at_ts}")
    return "\n".join(lines)


# -----------------------------------------------------------------------------
# Validation and guards
# -----------------------------------------------------------------------------


def require_feed_exists(contract: AlphaScanContract, feed_id: int) -> AlphaFeedConfig:
    cfg = contract.get_feed(feed_id)
    if cfg is None:
        raise ValueError(ASCError.FEED_CAP)
    return cfg


def require_feed_active(contract: AlphaScanContract, feed_id: int) -> AlphaFeedConfig:
    cfg = require_feed_exists(contract, feed_id)
    if not cfg.active:
        raise ValueError(ASCError.SCRAPE_DISABLED)
    return cfg


def require_not_locked(contract: AlphaScanContract) -> None:
    if contract.locked:
        raise ValueError(ASCError.RADAR_LOCKED)


def require_valid_score(score: int) -> None:
    if not (SCORE_MIN <= score <= SCORE_MAX):
        raise ValueError(ASCError.INVALID_SCORE)


# -----------------------------------------------------------------------------
# Terminal table builders
# -----------------------------------------------------------------------------


def build_feed_table(contract: AlphaScanContract) -> List[str]:
    lines = ["feed_id | source_tag           | relay       | active"]
    for f in sorted(contract._feeds.values(), key=lambda x: x.feed_id):
        lines.append(format_feed_line(f))
    return lines


def build_pulse_table(contract: AlphaScanContract, feed_id: Optional[int] = None, limit: int = 30) -> List[str]:
    lines = ["pulse_id | feed_id | score  | payload_hash      | emitted_at_ts"]
    pulses = list(contract._pulses.values())
    if feed_id is not None:
        pulses = [p for p in pulses if p.feed_id == feed_id]
    pulses.sort(key=lambda p: p.emitted_at_ts, reverse=True)
    for p in pulses[:limit]:
        lines.append(format_pulse_line(p))
    return lines


def build_signal_table(contract: AlphaScanContract, feed_id: Optional[int] = None, limit: int = 30) -> List[str]:
    lines = ["signal_id | feed_id | author_handle     | score  | at_ts"]
    signals = list(contract._signals.values())
    if feed_id is not None:
        signals = [s for s in signals if s.feed_id == feed_id]
    signals.sort(key=lambda s: s.at_ts, reverse=True)
    for s in signals[:limit]:
        lines.append(format_signal_line(s))
    return lines


def build_radar_table(contract: AlphaScanContract) -> List[str]:
    lines = ["slot_index | pulse_id | feed_id | score | at_ts"]
    for s in contract.get_radar_slots():
        lines.append(format_radar_slot(s))
    return lines


# -----------------------------------------------------------------------------
# Time-window queries
# -----------------------------------------------------------------------------


def pulses_in_window(contract: AlphaScanContract, from_ts: int, to_ts: int) -> List[RadarPulse]:
    return [p for p in contract._pulses.values() if from_ts <= p.emitted_at_ts <= to_ts]


def signals_in_window(contract: AlphaScanContract, from_ts: int, to_ts: int) -> List[SocialSignal]:
    return [s for s in contract._signals.values() if from_ts <= s.at_ts <= to_ts]


def pulse_count_per_feed(contract: AlphaScanContract) -> Dict[int, int]:
    out: Dict[int, int] = {}
    for p in contract._pulses.values():
        out[p.feed_id] = out.get(p.feed_id, 0) + 1
    return out


def signal_count_per_feed(contract: AlphaScanContract) -> Dict[int, int]:
    out: Dict[int, int] = {}
    for s in contract._signals.values():
        out[s.feed_id] = out.get(s.feed_id, 0) + 1
    return out


# -----------------------------------------------------------------------------
# Hash and ID helpers
# -----------------------------------------------------------------------------


def short_hash(h: str, prefix_len: int = 8, suffix_len: int = 4) -> str:
    if not h or len(h) < prefix_len + suffix_len:
        return h
    if h.startswith("0x"):
        return "0x" + h[2:2+prefix_len] + "…" + h[-suffix_len:]
    return h[:prefix_len] + "…" + h[-suffix_len:]


def parse_feed_id(s: str) -> Optional[int]:
    try:
        return int(s.strip())
    except ValueError:
        return None


def parse_pulse_id(s: str) -> Optional[int]:
    try:
        return int(s.strip())
    except ValueError:
        return None


def parse_signal_id(s: str) -> Optional[int]:
    try:
        return int(s.strip())
    except ValueError:
        return None


# -----------------------------------------------------------------------------
# Mock data generators for testing
# -----------------------------------------------------------------------------


def generate_mock_feeds(n: int) -> List[Tuple[str, str]]:
    sources = ["x_crypto", "x_defi", "discord_alpha", "telegram_signal", "reddit_cc"]
    return [(f"feed_{sources[i % len(sources)]}_{i}", ALPHA_SCAN_RELAY) for i in range(n)]


def generate_mock_pulse(feed_id: int, score: Optional[int] = None) -> Dict[str, Any]:
    return {
        "feed_id": feed_id,
        "payload_hash": content_hash(str(time.time())),
        "score": score if score is not None else (SCORE_MIN + (int(time.time()) % (SCORE_MAX - SCORE_MIN))),
        "emitted_at_ts": int(time.time()),
    }


def generate_mock_signal(feed_id: int, author: str = "user") -> Dict[str, Any]:
    return {
        "feed_id": feed_id,
        "content_hash": content_hash(author + str(time.time())),
        "author_handle": author,
        "score": int(time.time()) % (SCORE_MAX - SCORE_MIN) + SCORE_MIN,
        "at_ts": int(time.time()),
    }


# -----------------------------------------------------------------------------
# Export
# -----------------------------------------------------------------------------

__all__ = [
    "AlphaScanContract",
    "AlphaFeedConfig",
    "RadarPulse",
    "SocialSignal",
    "ScrapeItem",
    "RadarSlot",
    "ASCError",
    "ASCEvent",
    "ALPHA_SCAN_RELAY",
    "ALPHA_SCAN_GUARDIAN",
    "ALPHA_SCAN_TREASURY",
    "ALPHA_SCAN_FALLBACK",
    "ALPHA_SCAN_NAMESPACE",
    "ALPHA_SCAN_VERSION",
    "content_hash",
    "payload_hash",
    "score_from_engagement",
    "normalize_handle",
    "validate_address",
    "mock_scrape_item",
    "mock_pulse_payload",
    "MAX_FEEDS",
    "MAX_PULSE_AGE_SEC",
    "SCORE_MIN",
    "SCORE_MAX",
    "BATCH_SCRAPE_MAX",
    "RADAR_SLOT_COUNT",
    "FeedSourceType",
    "FEED_SOURCE_NAMES",
    "feed_source_from_tag",
    "filter_pulses_by_score",
    "filter_signals_by_author",
    "aggregate_score_for_feed",
    "top_n_pulses",
    "top_n_signals",
    "format_feed_line",
    "format_pulse_line",
    "format_signal_line",
    "format_radar_slot",
    "get_feeds_batch",
    "get_pulses_batch",
    "get_signals_batch",
    "get_config_snapshot",
    "get_feed_ids_active",
    "count_stale_pulses",
    "export_contract_snapshot",
    "export_feeds_csv",
    "export_pulses_csv",
    "export_signals_csv",
    "require_feed_exists",
    "require_feed_active",
    "require_not_locked",
    "require_valid_score",
    "build_feed_table",
    "build_pulse_table",
    "build_signal_table",
    "build_radar_table",
    "pulses_in_window",
    "signals_in_window",
    "pulse_count_per_feed",
    "signal_count_per_feed",
    "short_hash",
    "parse_feed_id",
    "parse_pulse_id",
    "parse_signal_id",
    "generate_mock_feeds",
    "generate_mock_pulse",
    "generate_mock_signal",
    "rank_feeds_by_pulse_count",
    "rank_feeds_by_avg_score",
    "rank_signals_by_score",
    "pulses_older_than",
    "pulses_newer_than",
    "latest_pulse_per_feed",
]


# -----------------------------------------------------------------------------
# Event log simulation (terminal-style audit trail)
# -----------------------------------------------------------------------------


@dataclass
class EventLogEntry:
    event_type: str
    at_ts: int
    payload: Dict[str, Any]


_event_log: List[EventLogEntry] = []


def log_event(event_type: str, payload: Dict[str, Any]) -> None:
    _event_log.append(EventLogEntry(event_type=event_type, at_ts=int(time.time()), payload=payload))


def get_event_log(limit: int = 100) -> List[EventLogEntry]:
    return _event_log[-limit:] if _event_log else []

