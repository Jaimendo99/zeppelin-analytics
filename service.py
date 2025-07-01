from typing import List

import pandas as pd
from pydantic import BaseModel, Field
from metriccalc.stress import stress_report
from metriccalc.concentration import  get_concentration_score
from metriccalc.sessionlog import generate_session_log
from metriccalc.session_summary import get_number_of_sessions, get_average_session_time
from models import UserReport
from report_parse import _parse_stress_report, _parse_focus_report, _parse_session_log


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
    raw_stress = stress_report(df, user_id, start_date, end_date)
    raw_concentration = get_concentration_score(df, user_id, start_date, end_date)

    # For the log, we need to pick one session or combine logs.
    # Here, we'll just grab the first session ID from the stress report for demo.
    first_session_id = None
    if raw_stress["sub_stress"]:
        first_session_id = raw_stress["sub_stress"][0]["session_id"]

    raw_log = []
    if first_session_id:
        raw_log = generate_session_log(df, user_id, str(first_session_id))

    # 2. Parse the raw data into Pydantic models
    stress_model = _parse_stress_report(raw_stress)
    focus_model = _parse_focus_report(raw_concentration)
    log_model = _parse_session_log(raw_log)
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