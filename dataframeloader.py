from apiClient import APIClient
from db import get_database
import pandas as pd
from utils import parse_date, parse_body

lake = pd.DataFrame()

async def load_lake(api: APIClient = None,db =None):
    print('Loading lake...')
    global lake
    users = await api.get_users()
    userdf = pd.DataFrame(users)
    courses = await api.request(endpoint="/course")
    coursesdf = pd.DataFrame(courses)
    reports_collection = db['test']['reports']
    reports = pd.DataFrame(list(reports_collection.find({})))
    pre_lake = (pd.merge(userdf, reports, left_on='user_id', right_on='userId', how='right')
                .merge(coursesdf, left_on='courseId', right_on='course_id', how='left')
                )
    metric_columns = ['focus_gain.time', 'focus_lost.time', 'heartrate_change.count',
                      'heartrate_change.mean', 'heartrate_change.value',
                      'physical.detected_at', 'physical.speed', 'text_scroll.direction',
                      'text_scroll.distance', 'text_scroll.position', 'text_scroll.time',
                      'unpin_screen.at', 'video_jump.at', 'video_jump.direction',
                      'video_jump.to', 'video_paused.at', 'video_paused.duration',
                      'video_percentage.at', 'video_percentage.percentage',
                      'video_speed_changed.at', 'video_speed_changed.speed',
                      'weak_rssi.value', 'wearable_off.at']
    pre_lake['addedAt'] = pre_lake['addedAt'].apply(parse_date)
    body_df = pre_lake.apply(lambda row: parse_body(row['body'], row['type']), axis=1)
    processed_lake = pd.concat([pre_lake.drop(columns=['body']), body_df], axis=1)
    processed_lake = processed_lake.drop_duplicates(subset=metric_columns)
    lake = processed_lake.reset_index(drop=True)
    lake.to_csv('lake.csv')
    print('🖕🏼Lake loaded successfully with shape:', lake.shape, 'and columns:', lake.columns.tolist())
