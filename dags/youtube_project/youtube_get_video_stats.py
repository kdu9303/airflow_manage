import time
import datetime
import json
import pandas as pd
# Call youtube api
# export PYTHONPATH="${PYTHONPATH}:/opt/airflow/dags/"
from scripts.call_youtube_api import get_authenticated_service


def get_video_statistic(youtube: callable, video_list: list):
    """채널 비디오 통계를 dictionary형식으로 리턴한다."""
    video_data = {}

    for video_ids in video_list:
        request = youtube.videos().list(
            id=video_ids,
            part='id,statistics',
            fields='nextPageToken,items(id,statistics)',
            maxResults=50
        )

        while request:
            response = request.execute()

            for item in response['items']:
                video_id = item['id']
                video_item = item['statistics']
                video_data[video_id] = video_item

            # nextPageToken를 찾고 없으면 false를 반환한다
            request = youtube.search().list_next(
                request, response)

        # 요청 횟수 초과 방지
        time.sleep(1)

    return video_data



# def video_list_to_dataframe(video_data: dict) -> pd.DataFrame:
#     """채널 비디오 정보를 데이터프레임으로 리턴한다."""
#     df = pd.concat({k: pd.DataFrame(v) for k, v in video_data.items()}, axis=0)

#     # 멀티인덱스에서 level_0만 가져온다
#     # level_1인덱스는 high,low 해상도 구분자
#     df["video_id"] = df.index.get_level_values(0)

# #    df = df[["video_id","publishedAt","title","description"]]

#     # video id 기준으로 중복자료 제거(Multi Index로 인한 중복 발생)
#     df = df[~df.duplicated(subset = ["video_id"])].reset_index(drop=True)

#     # 날짜 칼럼 str에서 datetime으로 변경
#     df["publishedAt"] = pd.to_datetime(df["publishedAt"])
    
#     return df

# def youtube_main():

#     channel_id = 'UCIAUH22hoMwHsVCKCKTR7Hw'

#     # 기준일자 설정
#     # 밤 12시 기준으로 전날 일자가 찍히도록 한다.
#     base_date = datetime.datetime.now() - datetime.timedelta(days=1)
#     base_date = base_date.replace(hour=0, minute=0, second=0, microsecond=0)

#     # 초기화
#     youtube = get_authenticated_service()


#     # 비디오 통계 dictionary
#     video_statistic_dict = get_video_statistic(youtube, vid_list)


#     # print(json.dumps(channel_statistics_dict, indent=4))

#     # dict to dataframe
#     channel_statistics_df = pd.json_normalize(channel_statistics_dict.get('items')[0]["statistics"], errors='raise')
#     print(channel_statistics_df)


# youtube_main()
