# --------------------------------------------------------------- #
from __future__ import annotations

from typing import Any, Dict, List
from utils import parse_date
import pandas as pd

def generate_session_log(  # noqa: C901
        df: pd.DataFrame, user_id: str, session_id: int
) -> List[Dict[str, Any]]:
    """
    Generates a high-level, human-readable log for a specific study session.

    Args:
        df: The pre-processed DataFrame (`processed_lake`).
        user_id: The ID of the user to report on.
        session_id: The specific session ID to generate the log for.

    Returns:
        A list of dictionaries, where each dictionary represents a
        log entry, ready to be sent to an API.
    """
    mask = (df["userId"] == user_id) & (df["sessionId"] == session_id)
    session_df = df.loc[mask].sort_values("addedAt").reset_index(drop=True)

    if session_df.empty:
        return []

    log_entries: List[Dict[str, Any]] = []
    user_name = session_df["name"].iloc[0]

    def _to_ms_timestamp(ts: pd.Timestamp) -> int:
        """Converts a pandas Timestamp to an integer Unix timestamp in ms."""
        return int(ts.value / 1_000_000)

    # --- Add SESSION_START event ---
    start_time = session_df["addedAt"].min()
    log_entries.append({
        "session_id": int(session_id),
        "user_name": user_name,
        "event_type": "SESSION_START",
        "event_description": f"{user_name} started a study session.",
        "timestamp": _to_ms_timestamp(start_time),
    })

    # --- State tracking for aggregation ---
    is_stressed = False
    is_active = False
    STRESS_HR_THRESHOLD = 100  # BPM to be considered "stressed"
    CALM_HR_THRESHOLD = 80  # BPM to be considered "calm" again
    ACTIVITY_SPEED_THRESHOLD = 1.0  # m/s to be considered "active"

    # --- Process events chronologically ---
    for _, row in session_df.iterrows():
        event_type = row["type"]
        timestamp = _to_ms_timestamp(row["addedAt"])
        entry = None

        # --- Direct Mappings ---
        if event_type == "TAB_FOCUS_LOST":
            entry = {
                "event_type": "FOCUS_LOST",
                "event_description": f"{user_name} lost focus on the page.",
            }
        elif event_type == "TAB_FOCUS_GAIN":
            entry = {
                "event_type": "FOCUS_GAINED",
                "event_description": f"{user_name} returned to the page.",
            }
        elif event_type == "VIDEO_PAUSED" and row["video_paused.duration"] > 10:
            duration = int(row["video_paused.duration"])
            entry = {
                "event_type": "VIDEO_PAUSED",
                "event_description": f"Paused the video for {duration} seconds.",
            }
        elif event_type == "VIDEO_SPEED_CHANGED":
            speed = row["video_speed_changed.speed"]
            entry = {
                "event_type": "VIDEO_SPEED_CHANGED",
                "event_description": f"Changed video speed to {speed}x.",
            }
        elif event_type == "UNPIN_SCREEN":
            entry = {
                "event_type": "SCREEN_UNPINNED",
                "event_description": f"{user_name} unpinned the screen on their phone.",
            }
        elif event_type == "WEARABLE_OFF":
            entry = {
                "event_type": "WATCH_REMOVED",
                "event_description": f"{user_name} took off the smart watch.",
            }
        elif event_type == "WEAK_RSSI":
            entry = {
                "event_type": "WEAK_SIGNAL",
                "event_description": "A weak signal was detected, which may cause interruptions.",
            }

        # --- Aggregated Mappings (State Changes) ---
        elif event_type == "USER_HEARTRATE":
            hr = row["heartrate_change.mean"]
            if hr > STRESS_HR_THRESHOLD and not is_stressed:
                is_stressed = True
                entry = {
                    "event_type": "STRESS_INCREASED",
                    "event_description": "Detected an elevated heart rate, indicating a rise in stress.",
                }
            elif hr < CALM_HR_THRESHOLD and is_stressed:
                is_stressed = False
                entry = {
                    "event_type": "STRESS_DECREASED",
                    "event_description": "Heart rate has returned to a normal level.",
                }
        elif event_type == "USER_PHYSICAL_ACTIVITY":
            speed = row["physical.speed"]
            if speed > ACTIVITY_SPEED_THRESHOLD and not is_active:
                is_active = True
                entry = {
                    "event_type": "BECAME_ACTIVE",
                    "event_description": f"{user_name} became physically active (e.g., got up or started walking).",
                }
            elif speed < ACTIVITY_SPEED_THRESHOLD and is_active:
                is_active = False
                entry = {
                    "event_type": "BECAME_SEDENTARY",
                    "event_description": f"{user_name} is no longer physically active.",
                }

        if entry:
            entry.update({"session_id":int(session_id),"user_name": user_name, "timestamp": timestamp})
            log_entries.append(entry)

    # --- Add SESSION_END event ---
    end_time = session_df["addedAt"].max()
    log_entries.append({
        "session_id":int(session_id),
        "user_name": user_name,
        "event_type": "SESSION_END",
        "event_description": f"{user_name} ended the study session.",
        "timestamp": _to_ms_timestamp(end_time),
    })

    return log_entries


def get_all_logs(
        df: pd.DataFrame, user_id: str, start_date: str, end_date: str
) -> List[Dict[str, Any]]:
    """
    Generates logs for all sessions of a user within a date range.
    Returns a list of session logs.
    """
    mask = (df["userId"] == user_id) & (
            df["addedAt"] >= parse_date(start_date)
    ) & (df["addedAt"] < parse_date(end_date) + pd.Timedelta(days=1))
    user_df = df.loc[mask]

    if user_df.empty:
        return []

    session_ids = user_df["sessionId"].unique()
    all_logs = []
    for session_id in session_ids:
        log = generate_session_log(user_df, user_id, session_id)
        if log:
            all_logs.extend(log)

    return all_logs
