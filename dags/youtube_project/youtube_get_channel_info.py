import logging
from datetime import datetime
import pandas as pd
# Call youtube api
from scripts.call_youtube_api import get_authenticated_service

# BUILD 초기화
YOUTUBE = get_authenticated_service()


def get_channel_statistics(youtube: callable, channel_id: str) -> dict:
    """채널 통계 정보를 dictionary형식으로 반환한다."""

    try:
        request = youtube.channels().list(
            # id=channel_id,
            mine=True,
            part='id,statistics',
            fields='nextPageToken,items(id,statistics)',
        )

        channel_data = request.execute()

    except Exception as e:
        logging.info(e)
    else:
        return channel_data


def channel_statistics_to_df(channel_id, channel_stat: dict) -> pd.DataFrame:
    """채널 정보를 데이터프레임으로 리턴한다."""

    channel_stat_df = pd.json_normalize(
        channel_stat.get('items')[0]["statistics"], errors='raise'
    )

    channel_stat_df = \
        channel_stat_df[["viewCount", "subscriberCount", "videoCount"]]

    # 숫자형으로 변환
    channel_stat_df[["viewCount", "subscriberCount", "videoCount"]] = \
        channel_stat_df[["viewCount", "subscriberCount", "videoCount"]]\
        .astype(int)
        #  .apply(pd.to_numeric)

    channel_stat_df["channelId"] = channel_id

    # 기준일자 설정
    base_date = pd.to_datetime(datetime.today().replace(hour=0,
                                                        minute=0,
                                                        second=0,
                                                        microsecond=0))
    
    channel_stat_df["baseDate"] = base_date

    # 칼럼 순서 정의
    # MERGE 문은 첫번째 칼럼 순서대로 삽입됨으로 BIND 변수순서대로 칼럼을 바꾼다
    col_order = [
        "channelId", "baseDate", "viewCount",
        "subscriberCount", "videoCount"
    ]
    channel_stat_df = channel_stat_df[col_order]
    return channel_stat_df


# main
def return_channel_statistics() -> pd.DataFrame:

    channel_id = 'UCIAUH22hoMwHsVCKCKTR7Hw'

    # 채널 정보 dictonary
    channel_stat_dict = get_channel_statistics(YOUTUBE, channel_id)

    channel_stat_df = channel_statistics_to_df(channel_id, channel_stat_dict)
    print(channel_stat_df)

    return channel_stat_df


if __name__ == '__main__':
    return_channel_statistics()
    # request = YOUTUBE.channels().list(
    #     mine=True,
    #     part='id,statistics',
    #     fields='nextPageToken,items(id,statistics)',
    # )

    # channel_data = request.execute()
    # print(channel_data)
