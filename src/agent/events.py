# -*- coding: utf-8 -*-
"""
EventMonitor — lightweight event-driven alert system.

Monitors a set of stocks for threshold events and triggers
notifications when conditions are met.  Designed to run as a
background task (e.g. via ``--schedule`` or a dedicated loop).

Supported events:
- Price crossing threshold (above / below)
- Volume spike (> N× average)
- Sentiment shift (positive→negative or vice-versa)
- Risk flag activation (from RiskAgent)

Usage::

    from src.agent.events import EventMonitor, PriceAlert
    monitor = EventMonitor()
    monitor.add_alert(PriceAlert(stock_code="600519", direction="above", price=1800.0))
    triggered = await monitor.check_all()
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class AlertType(str, Enum):
    PRICE_CROSS = "price_cross"
    VOLUME_SPIKE = "volume_spike"
    SENTIMENT_SHIFT = "sentiment_shift"
    RISK_FLAG = "risk_flag"
    CUSTOM = "custom"


class AlertStatus(str, Enum):
    ACTIVE = "active"
    TRIGGERED = "triggered"
    EXPIRED = "expired"
    DISMISSED = "dismissed"


@dataclass
class AlertRule:
    """Base alert rule definition."""
    stock_code: str
    alert_type: AlertType
    description: str = ""
    status: AlertStatus = AlertStatus.ACTIVE
    created_at: float = field(default_factory=time.time)
    triggered_at: Optional[float] = None
    ttl_hours: float = 24.0  # auto-expire after this many hours
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PriceAlert(AlertRule):
    """Alert when price crosses a threshold."""
    alert_type: AlertType = AlertType.PRICE_CROSS
    direction: str = "above"  # "above" or "below"
    price: float = 0.0

    def __post_init__(self):
        if not self.description:
            self.description = f"{self.stock_code} price {self.direction} {self.price}"


@dataclass
class VolumeAlert(AlertRule):
    """Alert when volume exceeds N× average."""
    alert_type: AlertType = AlertType.VOLUME_SPIKE
    multiplier: float = 2.0  # trigger when volume > multiplier × avg

    def __post_init__(self):
        if not self.description:
            self.description = f"{self.stock_code} volume > {self.multiplier}× average"


@dataclass
class SentimentAlert(AlertRule):
    """Alert on sentiment direction change."""
    alert_type: AlertType = AlertType.SENTIMENT_SHIFT
    from_sentiment: str = "positive"  # "positive", "negative", "neutral"
    to_sentiment: str = "negative"

    def __post_init__(self):
        if not self.description:
            self.description = f"{self.stock_code} sentiment shift: {self.from_sentiment} → {self.to_sentiment}"


@dataclass
class TriggeredAlert:
    """An alert that was triggered, ready for notification."""
    rule: AlertRule
    triggered_at: float = field(default_factory=time.time)
    current_value: Any = None
    message: str = ""


class EventMonitor:
    """Monitor stocks for event-driven alerts.

    This class manages a list of :class:`AlertRule` objects and checks
    them against current market data.  Triggered alerts are collected
    and can be forwarded to the notification system.
    """

    def __init__(self):
        self.rules: List[AlertRule] = []
        self._callbacks: List[Callable[[TriggeredAlert], None]] = []

    def add_alert(self, rule: AlertRule) -> None:
        """Register a new alert rule."""
        self.rules.append(rule)
        logger.info("[EventMonitor] Added alert: %s", rule.description)

    def remove_expired(self) -> int:
        """Remove alerts that have expired based on TTL.

        Returns:
            Number of expired alerts removed.
        """
        now = time.time()
        before = len(self.rules)
        self.rules = [
            r for r in self.rules
            if r.status != AlertStatus.EXPIRED
            and (now - r.created_at) < r.ttl_hours * 3600
        ]
        removed = before - len(self.rules)
        if removed:
            logger.info("[EventMonitor] Removed %d expired alerts", removed)
        return removed

    def on_trigger(self, callback: Callable[[TriggeredAlert], None]) -> None:
        """Register a callback for when an alert triggers."""
        self._callbacks.append(callback)

    async def check_all(self) -> List[TriggeredAlert]:
        """Check all active rules against current market data.

        Returns:
            List of triggered alerts.
        """
        self.remove_expired()
        triggered: List[TriggeredAlert] = []

        for rule in self.rules:
            if rule.status != AlertStatus.ACTIVE:
                continue

            try:
                result = await self._check_rule(rule)
                if result:
                    triggered.append(result)
                    rule.status = AlertStatus.TRIGGERED
                    rule.triggered_at = time.time()
                    # Notify callbacks
                    for cb in self._callbacks:
                        try:
                            cb(result)
                        except Exception as exc:
                            logger.warning("[EventMonitor] Callback error: %s", exc)
            except Exception as exc:
                logger.debug("[EventMonitor] Check failed for %s: %s", rule.description, exc)

        return triggered

    async def _check_rule(self, rule: AlertRule) -> Optional[TriggeredAlert]:
        """Check a single rule.  Returns TriggeredAlert if condition met."""
        if isinstance(rule, PriceAlert):
            return await self._check_price(rule)
        elif isinstance(rule, VolumeAlert):
            return await self._check_volume(rule)
        # SentimentAlert and custom alerts require more context —
        # implemented as hooks for future extension
        return None

    async def _check_price(self, rule: PriceAlert) -> Optional[TriggeredAlert]:
        """Check price alert against realtime quote."""
        try:
            from data_provider import get_realtime_quote
            quote = get_realtime_quote(rule.stock_code)
            if quote is None:
                return None

            current_price = float(quote.get("current_price", 0))
            if current_price <= 0:
                return None

            triggered = False
            if rule.direction == "above" and current_price >= rule.price:
                triggered = True
            elif rule.direction == "below" and current_price <= rule.price:
                triggered = True

            if triggered:
                return TriggeredAlert(
                    rule=rule,
                    current_value=current_price,
                    message=f"🔔 {rule.stock_code} price {rule.direction} {rule.price}: "
                            f"current = {current_price}",
                )
        except Exception as exc:
            logger.debug("[EventMonitor] _check_price error: %s", exc)
        return None

    async def _check_volume(self, rule: VolumeAlert) -> Optional[TriggeredAlert]:
        """Check volume spike against recent average."""
        try:
            from data_provider import get_daily_history
            df = get_daily_history(rule.stock_code, days=20)
            if df is None or df.empty:
                return None

            avg_vol = df["volume"].mean()
            latest_vol = df["volume"].iloc[-1]

            if avg_vol > 0 and latest_vol > avg_vol * rule.multiplier:
                return TriggeredAlert(
                    rule=rule,
                    current_value=latest_vol,
                    message=f"📊 {rule.stock_code} volume spike: "
                            f"{latest_vol:,.0f} ({latest_vol / avg_vol:.1f}× avg)",
                )
        except Exception as exc:
            logger.debug("[EventMonitor] _check_volume error: %s", exc)
        return None

    # -----------------------------------------------------------------
    # Persistence helpers
    # -----------------------------------------------------------------

    def to_dict_list(self) -> List[Dict[str, Any]]:
        """Serialize all active rules for persistence."""
        results = []
        for rule in self.rules:
            entry: Dict[str, Any] = {
                "stock_code": rule.stock_code,
                "alert_type": rule.alert_type.value,
                "description": rule.description,
                "status": rule.status.value,
                "created_at": rule.created_at,
                "ttl_hours": rule.ttl_hours,
            }
            if isinstance(rule, PriceAlert):
                entry["direction"] = rule.direction
                entry["price"] = rule.price
            elif isinstance(rule, VolumeAlert):
                entry["multiplier"] = rule.multiplier
            results.append(entry)
        return results

    @classmethod
    def from_dict_list(cls, data: List[Dict[str, Any]]) -> "EventMonitor":
        """Restore an EventMonitor from serialized data."""
        monitor = cls()
        for entry in data:
            alert_type = entry.get("alert_type", "custom")
            stock_code = entry.get("stock_code", "")
            if alert_type == AlertType.PRICE_CROSS.value:
                rule = PriceAlert(
                    stock_code=stock_code,
                    direction=entry.get("direction", "above"),
                    price=entry.get("price", 0.0),
                )
            elif alert_type == AlertType.VOLUME_SPIKE.value:
                rule = VolumeAlert(
                    stock_code=stock_code,
                    multiplier=entry.get("multiplier", 2.0),
                )
            else:
                rule = AlertRule(
                    stock_code=stock_code,
                    alert_type=AlertType(alert_type) if alert_type in AlertType.__members__.values() else AlertType.CUSTOM,
                    description=entry.get("description", ""),
                )
            rule.status = AlertStatus(entry.get("status", "active"))
            rule.created_at = entry.get("created_at", time.time())
            rule.ttl_hours = entry.get("ttl_hours", 24.0)
            monitor.rules.append(rule)
        return monitor
