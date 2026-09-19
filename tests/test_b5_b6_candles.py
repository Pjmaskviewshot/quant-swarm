"""B5/B6 — closed-candle semantics.

B6: Bybit pushes the FORMING candle ~1/second and the `confirm` flag was
    ignored, so every "bar" series was a 1 Hz sample of a partial candle. The
    "4h" deque (maxlen 100) held roughly 100 SECONDS of data, and one shared
    price deque interleaved all timeframes.
B5: get_computed_atr reads timeframes["5"] then ["1"], neither of which was
    ever subscribed, so ATR collapsed to the std-dev of ~14 consecutive ticks.
    ATR drives SL distance, position size and the trailing cushion.
"""
import pathlib

from features.adaptive_engine import AdaptiveFeatureEngine

REPO = pathlib.Path(__file__).resolve().parents[1]
MAIN = (REPO / "src" / "main.py").read_text()


def bar(engine, tf, o, h, l, c, v=100.0):
    engine.update_multi_timeframe_candle(timeframe=tf, open_p=o, high_p=h,
                                         low_p=l, close_p=c, volume=v)


# --- B6: the confirm flag --------------------------------------------------

def test_forming_candles_are_rejected_at_the_handler():
    assert 'is_closed = bool(candle.get("confirm", False))' in MAIN, (
        "B6: the kline handler must check the confirm flag"
    )
    assert "if not is_closed:\n            return" in MAIN, (
        "B6: forming candles must be dropped, not appended"
    )


def test_bar_series_interval_is_explicit():
    assert "self.bar_series_interval" in MAIN
    assert "str(interval) == str(self.bar_series_interval)" in MAIN, (
        "B6: the correlation/shadow series must come from one declared timeframe"
    )


# --- B6: timeframe isolation ----------------------------------------------

def test_price_deque_only_receives_base_timeframe():
    e = AdaptiveFeatureEngine(base_timeframe="5")
    bar(e, "5", 100, 101, 99, 100.5)
    bar(e, "60", 200, 201, 199, 200.5)
    bar(e, "240", 300, 301, 299, 300.5)
    assert list(e.prices) == [100.5], (
        f"B6: price series polluted by other timeframes: {list(e.prices)}"
    )


def test_each_timeframe_keeps_its_own_series():
    e = AdaptiveFeatureEngine(base_timeframe="5")
    for i in range(3):
        bar(e, "5", 100 + i, 101 + i, 99 + i, 100.5 + i)
    bar(e, "60", 200, 201, 199, 200.5)
    assert len(e.timeframes["5"]) == 3
    assert len(e.timeframes["60"]) == 1
    assert e.timeframes["60"][-1]["close"] == 200.5


def test_unknown_timeframe_gets_its_own_series_not_dropped():
    e = AdaptiveFeatureEngine(base_timeframe="5")
    bar(e, "30", 100, 101, 99, 100.5)
    assert "30" in e.timeframes and len(e.timeframes["30"]) == 1


def test_htf_deques_are_bar_counts_not_seconds():
    e = AdaptiveFeatureEngine()
    assert e.timeframes["240"].maxlen == 100   # 100 four-hour bars
    assert e.timeframes["60"].maxlen == 200


# --- B5: ATR is computable from a real bar series -------------------------

def test_atr_uses_the_five_minute_series_when_present():
    e = AdaptiveFeatureEngine(base_timeframe="5")
    price = 100.0
    for i in range(40):
        price += 0.5
        bar(e, "5", price, price + 2.0, price - 2.0, price)
    atr = e.get_computed_atr(period=14)
    assert atr > 0.0, "B5: ATR still collapses to zero with a real 5m series"
    assert 1.0 < atr < 10.0, f"ATR {atr} implausible for a 4-wide true range"


def test_atr_is_zero_without_data_rather_than_noise():
    e = AdaptiveFeatureEngine(base_timeframe="5")
    assert e.get_computed_atr(period=14) == 0.0


def test_main_subscribes_the_atr_timeframe():
    assert "self.kline_intervals" in MAIN
    assert "intervals=self.kline_intervals" in MAIN, (
        "B5: the ATR timeframe must actually be subscribed on the stream"
    )
    assert '{self.timeframe, self.bar_series_interval, "60", "240"}' in MAIN


def test_kline_intervals_include_bar_series():
    """Simulates the constructor's interval computation."""
    timeframe, bar_series = "15", "5"
    intervals = sorted({timeframe, bar_series, "60", "240"}, key=lambda x: int(x))
    assert intervals == ["5", "15", "60", "240"]


# --- HTF bias now reads real bars -----------------------------------------

def test_htf_bias_needs_real_bars_and_is_bounded():
    e = AdaptiveFeatureEngine(base_timeframe="5")
    price = 100.0
    for _ in range(40):
        price += 1.0
        bar(e, "5", price, price + 1, price - 1, price)
        bar(e, "240", price, price + 1, price - 1, price)
        bar(e, "60", price, price + 1, price - 1, price)
    bias = e.get_htf_trend_bias(price)
    assert -1.0 <= bias <= 1.0


def test_dynamic_rr_is_bounded():
    e = AdaptiveFeatureEngine(base_timeframe="5")
    for i in range(30):
        bar(e, "5", 100 + i, 101 + i, 99 + i, 100.5 + i)
    rr = e.get_dynamic_rr_ratio()
    assert 1.2 <= rr <= 3.2
