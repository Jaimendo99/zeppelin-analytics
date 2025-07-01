from typing import List

from pydantic import Field, BaseModel


class FocusDetails(BaseModel):
    session_id: str = Field(..., description="ID of the session")
    text_scroll: float = Field(... , description="Text scroll concentration score")
    video_jump: float = Field(..., description="Video jump concentration score")
    video_pause: float = Field(..., description="Video pause concentration score")
    video_speed: float = Field(..., description="Video speed concentration score")
    tab_focus: float = Field(..., description="Tab focus concentration score")
    physical_activity: float = Field(..., description="Physical activity concentration score")
    weak_signal: float = Field(..., description="Weak signal concentration score")
    wearable_off: float = Field(..., description="Wearable off concentration score")

class FocusReport(BaseModel):
    focus_score: float = Field(..., description="Concentration score for the user")
    focus_details: List[FocusDetails] = Field(..., description="Detailed focus metrics for the user")


class StressDetails(BaseModel):
    session_id: str = Field( ..., description="Unique identifier for the monitoring session", )
    heart_rate: float = Field( ..., description="User's heart rate in beats per minute", )
    physical_activity_level: float = Field( ..., description="Physical activity intensity score (0-100)", )
    scroll_frequency: int = Field( ..., description="Number of scroll events per minute", )
    mouse_jumps: int = Field( ..., description="Number of erratic mouse movements detected", )
    focus_loss_count: int = Field( ..., description="Number of times user lost focus from the main window", )

class StressReport(BaseModel):
    overall_stress: float = Field(..., description="Overall stress score for the user")
    session_details: List[StressDetails] = Field(..., description="Detailed stress metrics per session")

class SessionLogItem(BaseModel):
    session_id: str = Field(..., description="Unique identifier for the monitoring session")
    user_name: str = Field(..., description="Name of the user")
    event_type: str = Field(..., description="Type of event (e.g., 'VIDEO_PLAY', 'TEXT_SCROLL')")
    description: str = Field(..., description="Description of the event")
    timestamp: int = Field(..., description="Timestamp in Unix milliseconds")

class SessionLogReport(BaseModel):
    logs : List[SessionLogItem] = Field(..., description="List of session logs for the user")

class UserReport(BaseModel):
    session_count: int = Field(..., description="Number of sessions")
    average_session_time: float = Field(..., description="Average session time in seconds")
    focus_report: FocusReport = Field(..., description="Concentration report for the user")
    stress_report: StressReport = Field(..., description="Stress report for the user")
    session_log: SessionLogReport = Field(..., description="Session log report for the user")

