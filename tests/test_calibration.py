"""
Calibration and exit-quality analysis — APEX sections 20-22.

Two questions this system had no way to answer about itself: whether p_up = 0.70
actually means 70%, and whether the exits are late or early.

Every test below builds a case with a KNOWN answer and checks the tool reports
it. A diagnostic that has never been shown to detect the thing it looks for is
decoration.
"""
import math
import random

import pytest

from research.calibration import (
    calibration_report, brier_skill_score, excursion_report,
    suggested_stop_from_mae,
)


def perfectly_calibrated(n=4000, seed=1):
    """Predictions that are true by construction: outcome drawn with prob p."""
    rng = random.Random(seed)
    preds, outs = [], []
    for _ in range(n):
        p = rng.uniform(0.05, 0.95)
        preds.append(p)
        outs.append(1 if rng.random() < p else 0)
    return preds, outs


# ===================== calibration ========================================

def test_a_perfectly_calibrated_model_scores_near_zero_error():
    rep = calibration_report(*perfectly_calibrated())
    assert rep.expected_calibration_error < 0.05, rep.render()
    assert "well calibrated" in rep.verdict()


def test_systematic_overconfidence_is_detected():
    """Every prediction inflated by 0.15 — the model believes its own hype."""
    preds, outs = perfectly_calibrated()
    inflated = [min(1.0, p + 0.15) for p in preds]
    rep = calibration_report(inflated, outs)
    assert rep.bias > 0.10
    assert rep.expected_calibration_error > 0.10
    assert "OVERCONFIDENT" in rep.verdict()


def test_a_constant_forecaster_is_flagged_as_having_no_discrimination():
    """
    The failure mode a calibration metric alone will not catch: always
    predicting the base rate is PERFECTLY calibrated and completely useless.
    """
    n = 2000
    outs = [1 if i % 2 == 0 else 0 for i in range(n)]
    rep = calibration_report([0.5] * n, outs)
    assert rep.expected_calibration_error < 0.01, "it really is well calibrated"
    assert rep.resolution < 0.01
    assert "NO DISCRIMINATION" in rep.verdict()


def test_log_loss_punishes_confident_errors_far_more_than_brier():
    """
    Why both are reported. A model that says 0.99 and is wrong is barely dinged
    by Brier and destroyed by log-loss — the correct relative weighting when the
    output drives position size.
    """
    confident_wrong = calibration_report([0.99] * 100, [0] * 100)
    mild_wrong = calibration_report([0.60] * 100, [0] * 100)
    assert confident_wrong.brier / mild_wrong.brier < 3.0
    assert confident_wrong.log_loss / mild_wrong.log_loss > 4.0


def test_max_calibration_error_catches_a_bad_high_confidence_bin():
    """
    ECE can look fine while the model is badly wrong exactly where it takes the
    biggest positions. MCE is what surfaces that.
    """
    preds = [0.5] * 1900 + [0.95] * 100
    outs = [1 if i % 2 == 0 else 0 for i in range(1900)] + [0] * 100
    rep = calibration_report(preds, outs)
    assert rep.expected_calibration_error < 0.06, "the bulk looks fine"
    assert rep.max_calibration_error > 0.85, "the confident bin is badly wrong"


def test_brier_skill_is_zero_for_a_base_rate_forecaster():
    outs = [1] * 600 + [0] * 400
    assert brier_skill_score([0.6] * 1000, outs) == pytest.approx(0.0, abs=1e-9)


def test_brier_skill_is_negative_for_a_model_worse_than_guessing():
    outs = [1] * 600 + [0] * 400
    backwards = [0.4] * 600 + [0.6] * 400
    assert brier_skill_score(backwards, outs) < 0.0


def test_brier_skill_is_positive_for_a_model_with_real_information():
    outs = [1] * 600 + [0] * 400
    informed = [0.8] * 600 + [0.2] * 400
    assert brier_skill_score(informed, outs) > 0.5


def test_reliability_bins_sum_to_the_sample():
    preds, outs = perfectly_calibrated(n=1500, seed=4)
    rep = calibration_report(preds, outs)
    assert sum(b["n"] for b in rep.bins) == len(preds)


def test_a_probability_of_exactly_one_lands_in_the_last_bin():
    rep = calibration_report([1.0] * 50, [1] * 50)
    assert rep.bins[-1]["n"] == 50


def test_small_samples_are_refused_rather_than_scored():
    assert "INSUFFICIENT DATA" in calibration_report([0.5] * 20, [1] * 20).verdict()


def test_empty_input_does_not_raise():
    assert calibration_report([], []).n == 0


def test_mismatched_lengths_raise_rather_than_truncate():
    with pytest.raises(ValueError, match="length mismatch"):
        calibration_report([0.5, 0.6], [1])


def test_out_of_range_predictions_are_clamped_not_crashed():
    rep = calibration_report([-0.5, 1.5, 0.5], [0, 1, 1])
    assert math.isfinite(rep.brier) and math.isfinite(rep.log_loss)


# ===================== exit quality =======================================

def trade(mfe, mae, realised):
    return {"mfe": mfe, "mae": mae, "realised": realised}


def test_late_exits_are_detected():
    """Trades reach +2.0 and close at +0.3 — the signal is fine, the exit is not."""
    rep = excursion_report([trade(2.0, 0.3, 0.3) for _ in range(100)])
    assert rep.capture_ratio == pytest.approx(0.15)
    assert "EXITS ARE LATE" in rep.verdict()
    assert rep.mean_giveback == pytest.approx(1.7)


def test_capture_is_judged_on_winners_not_diluted_by_the_win_rate():
    """
    A design error I made first time round: computing capture across ALL trades
    mixes exit quality with the win rate, so any system with a normal share of
    losers gets falsely flagged as exiting late. Winners here keep 67% of what
    they reached — good exits — while the all-trade figure is 33%.
    """
    trades = ([trade(1.2, 0.3, 0.8) for _ in range(60)]
              + [trade(0.3, 1.0, -0.5) for _ in range(40)])
    rep = excursion_report(trades)
    assert rep.capture_ratio == pytest.approx(0.8 / 1.2)
    assert rep.portfolio_capture_ratio == pytest.approx(0.28 / 0.84)
    assert "EXITS ARE LATE" not in rep.verdict()


def test_early_exits_are_detected():
    rep = excursion_report([trade(1.0, 0.2, 0.95) for _ in range(100)])
    assert "EXITS MAY BE EARLY" in rep.verdict()


def test_winners_turned_into_losers_is_detected():
    """
    The most actionable finding MFE/MAE produces: losing trades that were
    substantially profitable first. That is an exit defect, not an entry one.
    """
    trades = ([trade(0.5, 0.2, 0.4) for _ in range(30)]
              + [trade(1.8, 2.0, -1.0) for _ in range(70)])
    rep = excursion_report(trades)
    assert rep.losers_mfe == pytest.approx(1.8)
    assert "LOSERS WENT PROFITABLE FIRST" in rep.verdict()


def test_an_entry_problem_is_distinguished_from_an_exit_problem():
    """Trades that go straight against the position: the exit rule is irrelevant."""
    rep = excursion_report([trade(0.1, 1.5, -1.2) for _ in range(100)])
    assert rep.edge_ratio < 1.0
    assert "ENTRY timing" in rep.verdict()


def test_a_healthy_profile_reports_no_complaint():
    trades = ([trade(1.2, 0.3, 0.8) for _ in range(60)]
              + [trade(0.3, 1.0, -0.5) for _ in range(40)])
    v = excursion_report(trades).verdict()
    assert "LATE" not in v and "EARLY" not in v and "ENTRY" not in v


def test_mae_is_treated_as_a_magnitude_whichever_sign_it_arrives_with():
    a = excursion_report([trade(1.0, 0.5, 0.5)] * 50)
    b = excursion_report([trade(1.0, -0.5, 0.5)] * 50)
    assert a.mean_mae == b.mean_mae == pytest.approx(0.5)


def test_small_samples_are_refused():
    assert "INSUFFICIENT DATA" in excursion_report([trade(1, 1, 1)] * 5).verdict()


def test_empty_trades_do_not_raise():
    assert excursion_report([]).n == 0


def test_trades_missing_excursion_data_are_skipped_not_guessed():
    rep = excursion_report([trade(1.0, 0.5, 0.5), {"realised": 1.0}])
    assert rep.n == 1, "a trade with no MFE/MAE must not be counted with zeros"


def test_suggested_stop_covers_the_stated_share_of_winners():
    trades = [trade(2.0, i * 0.01, 1.0) for i in range(100)]
    stop = suggested_stop_from_mae(trades, percentile=0.90)
    survived = sum(1 for t in trades if abs(t["mae"]) <= stop)
    assert survived >= 90


def test_suggested_stop_returns_none_on_a_thin_sample():
    """Better no answer than an answer fitted to twelve trades."""
    assert suggested_stop_from_mae([trade(1, 0.5, 1)] * 12) is None


def test_suggested_stop_is_documented_as_in_sample():
    """It is a diagnostic. Copying it into config is in-sample optimisation."""
    import research.calibration as C
    doc = C.suggested_stop_from_mae.__doc__
    assert "in-sample" in doc and "diagnostic" in doc
