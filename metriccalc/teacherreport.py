from typing import Dict, Optional, List
import pandas as pd
from pydantic import BaseModel
from apiClient import APIClient
from metriccalc.concentration import get_concentration_score_no_filter
from metriccalc.stress import stress_score_

# 1. Define the Pydantic models for the output structure
class Student(BaseModel):
    user_id: str
    fullname: str
    completion_percentage: float
    concentration_score: float
    stress_score: float


class DailyConcentration(BaseModel):
    date: str
    courseId: int
    course_title: str
    concentration_score: float


class TeacherReport(BaseModel):
    avg_time_course: float # decimal
    students_table: List[Student]
    completed_course: float
    total_sessions: int
    concentration_per_course_and_day: List[DailyConcentration]


# 2. Update the function to return the TeacherReport model
async def get_teacher_report(
        api: APIClient,
        df: pd.DataFrame,
        teacher_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
) -> TeacherReport:
    """
    Generates a comprehensive report for a given teacher, returning a structured
    Pydantic model.

    Args:
        api: The API client to fetch data.
        df: The DataFrame containing event data.
        teacher_id: The ID of the teacher to generate the report for.
        start_date: The start date for the report period (YYYY-MM-DD).
        end_date: The end date for the report period (YYYY-MM-DD).

    Returns:
        A TeacherReport Pydantic model containing the calculated metrics.
    """
    if start_date:
        df = df[df["addedAt"] >= start_date]
    if end_date:
        df = df[df["addedAt"] <= end_date]

    df_filtered = df[df["teacher_id"] == teacher_id].copy()

    if df_filtered.empty:
        # Return a default report if no data is found
        return TeacherReport(
            avg_time_course=0.0,
            students_table=[],
            completed_course=0.0,
            total_sessions=0,
            concentration_per_course_and_day=[],
        )

    # --- Calculations (same as before) ---
    course_summary = await api.request(
        "GET", "/student_course_progress_view", query={"teacher_id": f"eq.{teacher_id}"}
    )
    if course_summary:
        course_summary_df = pd.DataFrame(course_summary)
        students_progress = (
            course_summary_df.groupby("user_id")["completion_percentage"]
            .mean()
            .reset_index()
        )
    else:
        students_progress = pd.DataFrame(columns=["user_id", "completion_percentage"])

    concentration = (
        df_filtered.groupby("user_id")
        .apply(
            lambda g: get_concentration_score_no_filter(g, g)["concentration_score"],
            include_groups=False,
        )
        .reset_index(name="concentration_score")
    )
    stress = (
        df_filtered.groupby("user_id")
        .apply(lambda g: stress_score_(g).get("stress", 0), include_groups=False)
        .reset_index(name="stress_score")
    )

    users = await api.get_users()
    if users:
        users_df = pd.DataFrame(users)
    else:
        users_df = pd.DataFrame(columns=["user_id", "name"])



    students_table_df = pd.merge(concentration, stress, on="user_id", how="outer")
    students_table_df = pd.merge(
        students_table_df, students_progress, on="user_id", how="outer"
    )
    students_table_df["completion_percentage"] = (
            students_table_df["completion_percentage"].fillna(0) / 100
    )
    students_table_df.fillna(0, inplace=True)

    students_table_user = pd.merge(students_table_df, users_df, on='user_id', how='left').drop(columns=['email', 'type_id'])
    students_table_user['fullname'] = students_table_user['name'] + ' ' + students_table_user['lastname']
    students_table_user.drop(columns=['name', 'lastname'], inplace=True)

    completed_course = students_table_df["completion_percentage"].mean()
    total_sessions = df_filtered["sessionId"].nunique()

    session_times = df_filtered.groupby(["sessionId", "course_id"])["addedAt"].apply(
        lambda x: x.max() - x.min()
    )
    avg_time_course = (
        0
        if session_times.empty
        else session_times.groupby("course_id").sum().mean().total_seconds()
    )

    df_filtered["date"] = df_filtered["addedAt"].dt.strftime("%d-%m-%Y")
    concentration_results = (
        df_filtered.groupby(["date", "courseId"])
        .apply(
            lambda g: get_concentration_score_no_filter(g, g)["concentration_score"],
            include_groups=False,
        )
        .reset_index(name="concentration_score")
    )

    courses = await api.request(endpoint="/course", query={"teacher_id": f"eq.{teacher_id}"})
    if courses:
        courses_df = pd.DataFrame(courses)[["title", "course_id"]]
        concentration_df = pd.merge(
            concentration_results,
            courses_df,
            left_on="courseId",
            right_on="course_id",
            how="left",
        ).drop(columns=["course_id"])
    else:
        concentration_df = concentration_results
        concentration_df["title"] = "Unknown"

    # --- Data Transformation to Pydantic Models ---

    # Convert DataFrames to lists of dictionaries
    student_records = students_table_user.to_dict(orient="records")

    concentration_df.rename(columns={"title": "course_title"}, inplace=True)
    concentration_records = concentration_df.to_dict(orient="records")

    # Create lists of Pydantic models
    students_list = [Student(**record) for record in student_records]
    daily_concentration_list = [
        DailyConcentration(**record) for record in concentration_records
    ]

    # --- Instantiate and Return the Final Report Model ---
    return TeacherReport(
        avg_time_course=avg_time_course,
        students_table=students_list,
        completed_course=completed_course,
        total_sessions=total_sessions,
        concentration_per_course_and_day=daily_concentration_list,
    )