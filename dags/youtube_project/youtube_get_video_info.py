import time
import pandas as pd
# Call youtube api
from scripts.call_youtube_api import get_authenticated_service


def get_channel_videos(youtube: callable, channel_id: str) -> dict:
    """#채널 비디오 정보를 dictionary형식으로 반환한다."""

    try:
        request = youtube.search().list(
            channelId=channel_id,
            part='id,snippet',
            type='video',
            order='date',
            fields='nextPageToken,items(id,snippet)',
            maxResults=50
        )

        video_data = {}

        while request:
            response = request.execute()

            for item in response['items']:
                video_id = item['id']['videoId']
                video_item = item['snippet']
                video_data[video_id] = video_item

            # nextPageToken를 찾고 없으면 false를 반환한다
            request = youtube.search().list_next(
                request, response)

            time.sleep(1)
    except Exception as e:
        print(e)
    else:
        return video_data


def video_list_to_dataframe(video_data: dict) -> pd.DataFrame:
    """채널 비디오 정보를 데이터프레임으로 리턴한다."""

    df = pd.concat({k: pd.DataFrame(v) for k, v in video_data.items()}, axis=0)

    # 멀티인덱스에서 level_0만 가져온다
    # level_1인덱스는 high,low 해상도 구분자
    df["video_id"] = df.index.get_level_values(0)

    df = df[[
        "video_id", "publishedAt", "title",
        "description", "publishTime", "channelTitle"
    ]]

    # video id 기준으로 중복자료 제거(Multi Index로 인한 중복 발생)
    df = df[~df.duplicated(subset=["video_id"])].reset_index(drop=True)

    # 날짜 칼럼 str에서 datetime으로 변경
    df["publishedAt"] = pd.to_datetime(df["publishedAt"])
    df["publishTime"] = pd.to_datetime(df["publishTime"])

    return df


def return_channel_videos() -> pd.DataFrame:

    channel_id = 'UCIAUH22hoMwHsVCKCKTR7Hw'

    # 초기화
    youtube = get_authenticated_service()

    # 비디오 정보 dictionary
    channel_videos_dict = get_channel_videos(youtube, channel_id)

    video_list_df = video_list_to_dataframe(channel_videos_dict)

    return video_list_df.columns
