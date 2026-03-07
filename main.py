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
