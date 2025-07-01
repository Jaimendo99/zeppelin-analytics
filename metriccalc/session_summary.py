from utils import _filter_data
import pandas as pd

def get_number_of_sessions(
        df: pd.DataFrame, user_id: str, start_date: str, end_date: str
) -> int:
    """Calculates the total number of unique study sessions for a user."""
    user_df = _filter_data(df, user_id, start_date, end_date)
    return user_df["sessionId"].nunique()


def get_average_session_time(
        df: pd.DataFrame, user_id: str, start_date: str, end_date: str
) -> float:
    """Calculates the average session time in seconds."""
    user_df = _filter_data(df, user_id, start_date, end_date)
    if user_df.empty:
        return 0.0

    session_times = user_df.groupby("sessionId")["addedAt"].agg(["min", "max"])
    session_durations = session_times["max"] - session_times["min"]
    avg_duration_seconds = session_durations.mean().total_seconds()
    return avg_duration_seconds

