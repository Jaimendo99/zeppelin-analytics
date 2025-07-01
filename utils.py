import pandas as pd

def _filter_data(
        df: pd.DataFrame, user_id: str, start_date: str, end_date: str
) -> pd.DataFrame:
    """Helper function to filter by user and INCLUSIVE date range."""
    # Use your parser to create timezone-aware start/end datetimes
    start = parse_date(start_date)
    # Add one day to the end date to make the range inclusive
    end = parse_date(end_date) + pd.Timedelta(days=1)

    mask = (
            (df["user_id"] == user_id)
            & (df["addedAt"] >= start)
            & (df["addedAt"] < end)
    )
    return df[mask]


def parse_date(date: str | int) -> pd.Timestamp:
    """
    Parses a date string or integer (timestamp) into a [pandas] Timestamp.
    Handles both string and integer inputs, if the input is a str it will adjust for time zone -5 (America/Bogota).
    """
    if isinstance(date, str):
        return pd.to_datetime(date, utc=True).tz_convert('America/Bogota')
    elif isinstance(date, int):
        return pd.to_datetime(date, unit='ms', utc=True).tz_convert('America/Bogota')
    else:
        raise ValueError("Date must be a string or an integer (timestamp).")


def parse_body(body:dict, event_type :str) -> pd.Series:

    if event_type is None: return pd.Series()

    if event_type == "USER_HEARTRATE": return pd.Series({
        'heartrate_change.value': body['heartrate_change']['value'],
        'heartrate_change.count': body['heartrate_change']['count'],
        'heartrate_change.mean': body['heartrate_change']['mean']
    })

    if event_type == "USER_PHYSICAL_ACTIVITY": return pd.Series({
        'physical.detected_at': parse_date(body['detected_at']),
        'physical.speed': body['speed'],
    })
    if event_type == "WEAK_RSSI": return pd.Series({
        "weak_rssi.value": body['rssi']
    })

    if event_type == "WEARABLE_OFF": return pd.Series({
        "wearable_off.at": parse_date(body['time'])
    })

    if event_type == "TEXT_SCROLL": return pd.Series({
        "text_scroll.direction": body['scroll_direction'],
        "text_scroll.distance": body['scroll_distance'],
        "text_scroll.position": body['current_scroll_position'],
        "text_scroll.time": parse_date(body['timestamp'])
    })

    if event_type == "TAB_FOCUS_GAIN": return pd.Series({
        "focus_gain.time": parse_date(body['timestamp']),
    })

    if event_type == "TAB_FOCUS_LOST": return pd.Series({
        "focus_lost.time": parse_date(body['timestamp']),
    })

    if event_type == "UNPIN_SCREEN":return pd.Series({
        "unpin_screen.at": parse_date(body['removed_at']),
    })

    if event_type == "VIDEO_PAUSED": return pd.Series({
        "video_paused.at": parse_date(body['timestamp']),
        "video_paused.duration": body['duration'],
    })

    if event_type == "VIDEO_JUMP": return pd.Series({
        "video_jump.at": parse_date(body['timestamp']),
        "video_jump.to": body['jump_to'],
        "video_jump.direction": body['direction'],
    })

    if event_type == "VIDEO_SPEED_CHANGED": return pd.Series({
        "video_speed_changed.at": parse_date(body['timestamp']),
        "video_speed_changed.speed": body['speed'],
    })

    if event_type == "VIDEO_PERCENTAGE": return pd.Series({
        "video_percentage.at": parse_date(body['timestamp']),
        "video_percentage.percentage": body['percentage'],
    })

    return pd.Series({
        'other_type': event_type
    })



