from __future__ import annotations
from typing import Dict, Any, List

import numpy as np
import pandas as pd
from utils import parse_date


# ----------------------------- helpers ------------------------- #
def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


# ------------------- metric functions (1 per weight) ----------- #
# Each function returns a score from 0 (calm) to 1 (stressed)

def _metric_stress_heartrate(df: pd.DataFrame) -> float:
    """
    Scales the average heart rate to a 0-1 stress score.
    Assumes a baseline of 75bpm (calm) and a high of 110bpm (stressed).
    """
    if df.empty:
        return 0.0
    # Use the mean heartrate reported by the device
    avg_hr = df["heartrate_change.mean"].mean()
    baseline_hr = 75.0
    high_hr = 110.0
    score = (avg_hr - baseline_hr) / (high_hr - baseline_hr)
    return _clamp(score)


def _metric_stress_activity(df: pd.DataFrame, thresh: float = 1.0) -> float:
    """
    Returns the percentage of time the user was moving faster than the
    sedentary threshold (e.g., walking/pacing).
    """
    if df.empty:
        return 0.0
    high_speed_events = (df["physical.speed"] > thresh).sum()
    return high_speed_events / len(df)


def _metric_stress_scrolling(df: pd.DataFrame) -> float:
    """
    Uses the Coefficient of Variation (CV) of scroll distances.
    A high CV (erratic scrolling) maps to a higher stress score.
    A CV of 1.5 or more is considered max stress.
    """
    if len(df) < 2:
        return 0.0
    dists = df["text_scroll.distance"].to_numpy(float)
    std = dists.std(ddof=1)
    mean = dists.mean()
    if mean == 0:
        return 0.0
    cv = std / mean
    # Scale the CV so that 1.5 maps to a score of 1.0
    return _clamp(cv / 1.5)


def _metric_stress_video_jump(
        df: pd.DataFrame, duration_minutes: float
) -> float:
    """
    Calculates stress based on the frequency of video jumps.
    More than 1.5 jumps/minute is considered high stress.
    """
    if df.empty or duration_minutes == 0:
        return 0.0
    jumps_per_minute = len(df) / duration_minutes
    # Scale the frequency, where 1.5 jumps/min is max stress
    return _clamp(jumps_per_minute / 1.5)


def _metric_stress_tab_focus(
        df: pd.DataFrame, duration_minutes: float
) -> float:
    """
    Calculates stress based on the frequency of losing tab focus.
    More than 1 focus loss event/minute is considered high stress.
    """
    if duration_minutes == 0:
        return 0.0
    losses = (df["type"] == "TAB_FOCUS_LOST").sum()
    losses_per_minute = losses / duration_minutes
    # Scale the frequency, where 1 loss/min is max stress
    return _clamp(losses_per_minute / 1.0)


def stress_report(
        df: pd.DataFrame, user_id: str, date_from: str | int, date_to: str | int
) -> Dict[str, Any]:
    """
    Calculates stress levels and returns a dictionary with the overall
    stress score and a detailed breakdown per session.
    """
    start = parse_date(date_from)
    end = parse_date(date_to)

    print("COLUMNAS DE EL DATAFRAME", df.columns)
    mask = (
            (df["user_id"] == user_id)
            & (df["addedAt"] >= start)
            & (df["addedAt"] <= end)
    )
    dfx = df.loc[mask].copy()
    if dfx.empty:
        # Return a default structure if no data is found
        return {"stress": 0.0, "sub_stress": []}
    return stress_score_(dfx)

def stress_score_(dfx: pd.DataFrame) -> Dict[str, Any]:
    sub_stress_list: List[Dict[str, Any]] = []
    session_stress_levels: List[float] = []
    weights: Dict[str, float] = {
        "heartrate": 0.40,
        "activity": 0.15,
        "scrolling": 0.15,
        "jumping": 0.10,
        "focus_loss": 0.20,
    }

    for session_id, g in dfx.groupby("sessionId"):
        session_start = g["addedAt"].min()
        session_end = g["addedAt"].max()
        duration_minutes = (session_end - session_start).total_seconds() / 60.0

        # Break out event-specific frames
        f_hr = g[g["type"] == "USER_HEARTRATE"]
        f_activity = g[g["type"] == "USER_PHYSICAL_ACTIVITY"]
        f_scroll = g[g["type"] == "TEXT_SCROLL"]
        f_jump = g[g["type"] == "VIDEO_JUMP"]

        metrics = {
            "heartrate": _metric_stress_heartrate(f_hr),
            "activity": _metric_stress_activity(f_activity),
            "scrolling": _metric_stress_scrolling(f_scroll),
            "jumping": _metric_stress_video_jump(f_jump, duration_minutes),
            "focus_loss": _metric_stress_tab_focus(g, duration_minutes),
        }

        stress_level = sum(metrics[k] * w for k, w in weights.items())
        session_stress_levels.append(stress_level)

        # Build the dictionary for this specific session
        session_data = {
            "session_id": session_id,
            "stress_level": stress_level,
            # Add individual metrics with uppercase keys
            **{key.upper(): value for key, value in metrics.items()},
        }
        sub_stress_list.append(session_data)

    # Calculate the overall average stress for the period
    overall_stress = (
        float(np.mean(session_stress_levels)) if session_stress_levels else 0.0
    )

    # Construct the final output dictionary
    return {"stress": overall_stress, "sub_stress": sub_stress_list}
