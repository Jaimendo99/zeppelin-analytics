from typing import List

import pandas as pd
from pydantic import BaseModel, Field
from metriccalc.stress import stress_report
from metriccalc.concentration import  get_concentration_score
from metriccalc.sessionlog import generate_session_log, get_all_logs
from metriccalc.session_summary import get_number_of_sessions, get_average_session_time
from models import UserReport
from report_parse import _parse_stress_report, _parse_focus_report, _parse_session_log
from utils import _filter_data


def get_user_report(
        df: pd.DataFrame, user_id: str, start_date: str, end_date: str
) -> UserReport:
    """
    Generates and parses a full user report into Pydantic models.

    NOTE: This function assumes you have the analysis functions
    (stress_report, get_concentration_score, generate_session_log)
    available in the scope.
    """
    # 1. Generate raw data from your analysis functions
    # These would be the actual function calls

    filtered = _filter_data(df, user_id, start_date, end_date)
    if filtered.empty:
        raise ValueError(f"No data found for user {user_id} between {start_date} and {end_date}")


    print("Generating raw stress report...✅", df.columns)
    raw_stress = stress_report(df, user_id, start_date, end_date)
    raw_concentration = get_concentration_score(df, user_id, start_date, end_date)
    raw_logs = get_all_logs(df, user_id, start_date, end_date)

    # 2. Parse the raw data into Pydantic models
    stress_model = _parse_stress_report(raw_stress)
    focus_model = _parse_focus_report(raw_concentration)
    log_model = _parse_session_log(raw_logs)
    session_count = get_number_of_sessions(df, user_id, start_date, end_date)
    average_session_time = get_average_session_time(df, user_id, start_date, end_date)

    # 3. Assemble the final report
    return UserReport(
        session_count=session_count,
        average_session_time=average_session_time,
        stress_report=stress_model,
        focus_report=focus_model,
        session_log=log_model,
    )