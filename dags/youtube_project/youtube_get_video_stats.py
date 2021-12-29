import time
import csv
from datetime import datetime
import pandas as pd
# Call youtube api
# from scripts.call_youtube_api import get_authenticated_service_using_api
from scripts.call_youtube_api import get_authenticated_service

# BUILD 초기화
YOUTUBE = get_authenticated_service()


def get_video_statistic(youtube: callable, video_list: list):
    """채널 비디오 통계를 dictionary형식으로 리턴한다."""
    video_data = {}

    # 테스트 시에는 행 제한을 걸어서 한다
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


def video_stat_to_dataframe(video_data: dict) -> pd.DataFrame:
    """채널 비디오 정보를 데이터프레임으로 리턴한다."""
    df = pd.concat({k: pd.DataFrame([v]) for k, v in video_data.items()},
                   axis=0)

    # 멀티인덱스에서 level_0만 가져온다
    # level_1인덱스는 high,low 해상도 구분자
    df["video_id"] = df.index.get_level_values(0)

    # # 기준일자 설정
    base_date = pd.to_datetime(datetime.today().replace(hour=0,
                                                        minute=0,
                                                        second=0,
                                                        microsecond=0))

    df["baseDate"] = base_date

    # 숫자형으로 변환
    df[["viewCount", "likeCount", "dislikeCount",
        "favoriteCount", "commentCount"]] = \
        df[["viewCount", "likeCount", "dislikeCount",
            "favoriteCount", "commentCount"]].astype(int)

    df = df[[
        "video_id", "baseDate", "viewCount", "likeCount",
        "dislikeCount", "favoriteCount", "commentCount"
    ]]

    # video id 기준으로 중복자료 제거(Multi Index로 인한 중복 발생)
    df = df[~df.duplicated(subset=["video_id"])].reset_index(drop=True)

    return df


# main
def return_video_stats() -> pd.DataFrame:

    # 초기화
    # youtube = get_authenticated_service_using_api()

    # 비디오 리스트 가져오기
    video_list_file = "/opt/airflow/dags/youtube_project/data/video_list.csv"
    with open(video_list_file, newline='') as f:
        video_list = list(csv.reader(f))
        video_list = [''.join(video) for video in video_list]

    # 비디오 통계 dictionary
    video_statistic_dict = get_video_statistic(YOUTUBE, video_list)

    # 검증용
    # import json
    # print(json.dumps(video_statistic_dict, indent=4))

    video_statistic_df = video_stat_to_dataframe(video_statistic_dict)

    return video_statistic_df


if __name__ == '__main__':
    return_video_stats()
