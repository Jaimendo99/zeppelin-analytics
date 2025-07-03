import pandas as pd
import numpy as np
from typing import Optional, Dict, Any

# --- 1. Constants and Configuration --
# Master weights for each component of the concentration score
METRIC_WEIGHTS = {
    "TEXT_SCROLL": 0.15,
    "VIDEO_JUMP": 0.15,
    "VIDEO_PAUSE": 0.1,
    "VIDEO_SPEED": 0.1,
    "TAB_FOCUS": 0.15,
    "PHYSICAL_ACTIVITY": 0.1,
    "WEAK_SIGNAL": 0.15,
    "WATCH_OFF": 0.1,
}

# Configuration for calculations
HRTC_DOWN_PX = 500
HRTC_UP_PX = 100
VIDEO_PAUSE_THRESHOLD_S = 60
VIDEO_PAUSE_MAX_S = 300
PHYSICAL_ACTIVITY_THRESHOLD_MS = 1.0


# --- 2. Helper Functions for Each Metric ---


def _calculate_text_scroll_score(session_df: pd.DataFrame) -> float:
    """Calculates the concentration score based on text scrolling behavior."""
    scroll_events = session_df[session_df["type"] == "TEXT_SCROLL"].dropna(
        subset=["text_scroll.distance"]
    )
    if scroll_events.empty:
        return 1.0  # Perfect score if no scrolling occurred

    # 2a. Consistency Score (based =on Coefficient of Variation)
    distances = scroll_events["text_scroll.distance"]
    if len(distances) < 2:
        consistency_score = 1.0  # Cannot calculate CV for < 2 events
    else:
        mean_dist = distances.mean()
        std_dist = distances.std()
        if mean_dist == 0:
            cv = 1.0  # Avoid division by zero; high CV if mean is 0
        else:
            cv = std_dist / mean_dist
        consistency_score = np.exp(-cv)  # Exponential decay for good 0-1 score

    # 2b. Scroll Quality Score
    def get_quality(row):
        direction_factor = 0.8 if row["text_scroll.direction"] == "up" else 1.0
        hrtc = HRTC_UP_PX if row["text_scroll.direction"] == "up" else HRTC_DOWN_PX
        # Cap score at 1 to prevent huge scrolls from giving extra credit
        return direction_factor * min(1, row["text_scroll.distance"] / hrtc)

    scroll_quality_score = scroll_events.apply(get_quality, axis=1).mean()

    # 2c. Final Combined Score
    return (0.15 * consistency_score) + (0.85 * scroll_quality_score)


def _calculate_video_jump_score(
        session_df: pd.DataFrame, user_history_df: pd.DataFrame
) -> float:
    """Calculates score based on deviation from user's avg jump frequency."""
    # Calculate historical average jumps per session for this user
    user_jumps_per_session = (
        user_history_df[user_history_df["type"] == "VIDEO_JUMP"]
        .groupby("sessionId")
        .size()
    )
    # Use a default of 5 if no history, to avoid penalizing new users
    ajs = user_jumps_per_session.mean() if not user_jumps_per_session.empty else 5.0

    # Jumps in the current session
    sjs = len(session_df[session_df["type"] == "VIDEO_JUMP"])

    if ajs == 0:
        return 0.0 if sjs > 0 else 1.0  # If avg is 0, any jump is bad

    score = max(0, 1 - (abs(sjs - ajs) / ajs))
    return score


def _calculate_video_pause_score(session_df: pd.DataFrame) -> float:
    """Calculates score based on long video pauses."""
    long_pauses = session_df[
        (session_df["type"] == "VIDEO_PAUSED")
        & (session_df["video_paused.duration"] > VIDEO_PAUSE_THRESHOLD_S)
        ]
    if long_pauses.empty:
        return 1.0

    # For each long pause, calculate a penalty. Then multiply all factors.
    penalties = long_pauses["video_paused.duration"].apply(
        lambda d: 1 - (min(d, VIDEO_PAUSE_MAX_S) / VIDEO_PAUSE_MAX_S)
    )
    return np.prod(penalties)


def _calculate_video_speed_score(session_df: pd.DataFrame) -> float:
    """Calculates score based on video playback speed."""
    speed_events = session_df[
        session_df["type"] == "VIDEO_SPEED_CHANGED"
        ].dropna(subset=["video_speed_changed.speed"])
    if speed_events.empty:
        return 1.0

    def score_speed(s):
        if s <= 1.25:
            return 1.0
        elif 1.25 < s <= 2.0:
            return 1 - 0.5 * (s - 1.25)
        else:
            return 0.0

    return speed_events["video_speed_changed.speed"].apply(score_speed).mean()


def _calculate_tab_focus_score(session_df: pd.DataFrame) -> float:
    """Calculates score based on time the tab was in focus."""
    session_start = session_df["addedAt"].min()
    session_end = session_df["addedAt"].max()
    total_duration = (session_end - session_start).total_seconds()

    if total_duration == 0:
        return 1.0

    focus_lost_events = session_df[session_df["type"] == "TAB_FOCUS_LOST"].sort_values(
        by="focus_lost.time"
    )
    focus_gain_events = session_df[session_df["type"] == "TAB_FOCUS_GAIN"].sort_values(
        by="focus_gain.time"
    )

    total_distraction_seconds = 0
    for _, lost_event in focus_lost_events.iterrows():
        # Find the first gain event that happened after this lost event
        gain_after = focus_gain_events[
            focus_gain_events["focus_gain.time"] > lost_event["focus_lost.time"]
            ]
        if not gain_after.empty:
            gain_time = gain_after.iloc[0]["focus_gain.time"]
            total_distraction_seconds += (
                    gain_time - lost_event["focus_lost.time"]
            ).total_seconds()
        else:
            # If no gain found, user was distracted until session end
            total_distraction_seconds += (
                    session_end - lost_event["focus_lost.time"]
            ).total_seconds()

    focused_duration = max(0, total_duration - total_distraction_seconds)
    return focused_duration / total_duration


def _calculate_physical_activity_score(session_df: pd.DataFrame) -> float:
    """Calculates score based on the user's physical movement."""
    activity_events = session_df[
        session_df["type"] == "USER_PHYSICAL_ACTIVITY"
        ].dropna(subset=["physical.speed"])
    if activity_events.empty:
        return 1.0

    sedentary_events = (
            activity_events["physical.speed"] <= PHYSICAL_ACTIVITY_THRESHOLD_MS
    ).sum()
    return sedentary_events / len(activity_events)


def _calculate_weak_signal_score(session_df: pd.DataFrame) -> float:
    """Calculates score based on network signal strength (RSSI)."""
    signal_events = session_df[session_df["type"] == "WEAK_RSSI"].dropna(
        subset=["weak_rssi.value"]
    )
    if signal_events.empty:
        return 1.0

    # Map RSSI from [-90 (bad), -70 (good)] to [0, 1]
    def score_rssi(rssi):
        score = (rssi + 90) / 20  # (-90 -> 0, -70 -> 1)
        return np.clip(score, 0, 1)  # Clamp result between 0 and 1

    return signal_events["weak_rssi.value"].apply(score_rssi).mean()


def _calculate_watch_off_score(session_df: pd.DataFrame) -> float:
    """Calculates score based on the number of times the watch was removed."""
    n_off = len(session_df[session_df["type"] == "WEARABLE_OFF"])
    return max(0, 1 - 0.5 * n_off)


# --- 3. Main Orchestration Function ---


def get_concentration_score( full_df: pd.DataFrame, user_id: str, start_date: str, end_date: str, ) -> Optional[Dict[str, Any]]:
    """
    Calculates the average concentration score for a given user over a date range.

    Args:
        full_df: The pre-processed DataFrame containing all user data.
        user_id: The ID of the user to generate the report for.
        start_date: The start of the report period (e.g., '2025-06-01').
        end_date: The end of the report period (e.g., '2025-06-30').

    Returns:
        The average concentration score (0-1) or None if no data is found.
    """
    start_ts = pd.to_datetime(start_date, utc=True).tz_convert("America/Bogota")
    end_ts = pd.to_datetime(end_date, utc=True).tz_convert(
        "America/Bogota"
    ) + pd.Timedelta(days=1)

    # Filter data for the specific user and date range
    user_period_df = full_df[
        (full_df["userId"] == user_id)
        & (full_df["addedAt"] >= start_ts)
        & (full_df["addedAt"] < end_ts)
        ].copy()

    if user_period_df.empty or "sessionId" not in user_period_df.columns:
        print("No data found for this user in the specified period.")
        return None

    # Get all historical data for this user for the video jump calculation
    user_history_df = full_df[full_df["userId"] == user_id].copy()
    return get_concentration_score_no_filter(user_period_df, user_history_df)


def get_concentration_score_no_filter(
        user_period_df: pd.DataFrame, user_history_df: pd.DataFrame
) -> Optional[Dict[str, Any]]:
    """
    Calculates the average concentration score for a given user's sessions
    within a specified period.

    Args:
        user_period_df: A DataFrame containing events for a specific user
                        within the desired reporting period. This DataFrame
                        should already be filtered by userId and date range.
        user_history_df: A DataFrame containing all historical events for
                         the specific user. This is used for metrics like
                         VIDEO_JUMP to compare against historical averages.

    Returns:
        A dictionary containing the average concentration score (0-1) and
        individual sub-scores for each session, or None if no valid data
        or sessions are found.
    """

    if user_period_df.empty or "sessionId" not in user_period_df.columns:
        print("No data found for this user in the specified period.")
        return None

    session_scores = []
    sub_scores_list = []
    for session_id, session_df in user_period_df.groupby("sessionId"):
        if session_df.empty:
            continue

        # Calculate sub-score for each metric
        # If a metric type is absent, it gets a perfect score of 1.0
        sub_scores = {
            "SESSION_ID": session_id,
            "TEXT_SCROLL": _calculate_text_scroll_score(session_df),
            "VIDEO_JUMP": _calculate_video_jump_score(session_df, user_history_df),
            "VIDEO_PAUSE": _calculate_video_pause_score(session_df),
            "VIDEO_SPEED": _calculate_video_speed_score(session_df),
            "TAB_FOCUS": _calculate_tab_focus_score(session_df),
            "PHYSICAL_ACTIVITY": _calculate_physical_activity_score(session_df),
            "WEAK_SIGNAL": _calculate_weak_signal_score(session_df),
            "WATCH_OFF": _calculate_watch_off_score(session_df),
        }
        sub_scores_list += [sub_scores]

        # Calculate the final weighted score for the session
        final_session_score = sum(
            sub_scores[metric] * weight for metric, weight in METRIC_WEIGHTS.items()
        )
        session_scores.append(final_session_score)

    if not session_scores:
        print("No valid sessions found to calculate a score.")
        return None

    # Return the average score across all sessions in the period
    return {
        "concentration_score": np.mean(session_scores),
        "sub_scores": sub_scores_list,
    }