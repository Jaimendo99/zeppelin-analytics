# filename: report_parser.py
# --------------------------------------------------------------- #
from typing import Any, Dict, List
from models import (
    FocusDetails,
    FocusReport,
    SessionLogItem,
    SessionLogReport,
    StressDetails,
    StressReport,
)

# Assume your analysis functions are in these files
# from stress import stress_report
# from concentration import get_concentration_score
# from session_log import generate_session_log


def _parse_session_log(
        raw_data: List[Dict[str, Any]]
) -> SessionLogReport:
    """Parses raw log data into a SessionLogReport Pydantic model."""
    parsed_logs = [
        SessionLogItem(
            session_id=str(item.get("session_id")),
            user_name=item.get("user_name"),
            event_type=item.get("event_type"),
            description=item.get("event_description"), # Key mapping
            timestamp=item.get("timestamp"),
        )
        for item in raw_data
    ]
    return SessionLogReport(logs=parsed_logs)


def _parse_stress_report(raw_data: Dict[str, Any]) -> StressReport:
    """Parses raw stress data into a StressReport Pydantic model."""
    parsed_details = []
    for item in raw_data.get("sub_stress", []):
        # Create a new dict with lowercase keys to match the Pydantic model
        details_data = {key.lower(): float(value) for key, value in item.items()}
        parsed_details.append(StressDetails(**details_data))

    return StressReport(
        overall_stress=float(raw_data.get("stress", 0.0)),
        session_details=parsed_details,
    )


def _parse_focus_report(raw_data: Dict[str, Any]) -> FocusReport:
    """Parses raw concentration data into a FocusReport Pydantic model."""
    parsed_details = []
    for item in raw_data.get("sub_scores", []):
        # Normalize keys to lowercase and cast values to float
        details_data = {
            key.lower().replace("session_id", "session_id"): float(value)
            for key, value in item.items()
        }
        # Pydantic needs 'session_id', not 'session_id'
        details_data["session_id"] = str(details_data.pop("session_id"))
        parsed_details.append(FocusDetails(**details_data))

    return FocusReport(
        focus_score=float(raw_data.get("concentration_score", 0.0)),
        focus_details=parsed_details,
    )
