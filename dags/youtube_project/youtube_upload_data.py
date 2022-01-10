# -*- coding: utf-8 -*-
import time
import logging
from airflow.exceptions import AirflowException
# database module
from scripts.call_database import ADW_connection_cx_oracle
# youtube module
from youtube_project.youtube_get_channel_info import (
    return_channel_statistics
)  # 채널 정보
from youtube_project.youtube_get_video_info import (
    return_channel_videos
)  # 채널 비디오 정보
from youtube_project.youtube_get_video_stats import (
    return_video_stats
)  # 비디오 통계


# external file 이 아닌 쿼리 작성은 upload 파일에 모아서 정의한다
def upload_yutube_channel_info(table_name: str):
    logging.info("함수를 호출합니다. upload_yutube_channel_info()")

    try:
        startTime = time.time()

        # 채널 정보를 가져온다.
        df = return_channel_statistics()

        with ADW_connection_cx_oracle() as conn:
            with conn.cursor() as cur:
                cur.fast_executemany = True

                rows = [tuple(x) for x in df.values]

                # bind 데이터 타입 확인 필수(특히 date형식)
                cur.executemany(
                    f"""MERGE INTO {table_name} a
                        USING DUAL
                        ON
                        (
                            a.CHANNELID = :1
                        AND a.BASEDATE = :2
                        )
                        WHEN MATCHED THEN
                            UPDATE SET
                                    a.VIEWCOUNT = :3
                                , a.SUBSCRIBERCOUNT = :4
                                , a.VIDEOCOUNT = :5
                        WHEN NOT MATCHED THEN
                        INSERT
                        (
                                a.CHANNELID
                            , a.BASEDATE
                            , a.VIEWCOUNT
                            , a.SUBSCRIBERCOUNT
                            , a.VIDEOCOUNT
                        )
                        VALUES (:1, :2, :3, :4, :5)
                    """, rows)
                conn.commit()

        endTime = time.time()

        logging.info("EXTRACT 성공")
        logging.info(f'실행 시간: {round(endTime-startTime, 2)}초')
    except Exception as e:
        raise AirflowException(
            f"{upload_yutube_channel_info.__name__} --> {e}"
            )


def upload_yutube_video_info(table_name: str):
    logging.info("함수를 호출합니다. upload_yutube_video_info()")

    try:
        startTime = time.time()

        # 비디오 정보를 가져온다.
        df = return_channel_videos()

        with ADW_connection_cx_oracle() as conn:
            with conn.cursor() as cur:
                cur.fast_executemany = True

                rows = [tuple(x) for x in df.values]

                # bind 데이터 타입 확인 필수(특히 date형식)
                cur.executemany(
                    f"""MERGE INTO {table_name} a
                        USING DUAL
                        ON
                        (
                            a.VIDEO_ID = :1
                        )
                        WHEN MATCHED THEN
                            UPDATE SET
                                    a.CHANNELTITLE = :2
                                , a.PUBLISHEDAT = :3
                                , a.PUBLISHTIME = :4
                                , a.DESCRIPTION = NVL(:5,'-')
                                , a.TITLE = :6
                        WHEN NOT MATCHED THEN
                        INSERT
                        (
                                a.VIDEO_ID
                            , a.CHANNELTITLE
                            , a.PUBLISHEDAT
                            , a.PUBLISHTIME
                            , a.DESCRIPTION
                            , a.TITLE
                        )
                        VALUES (:1, :2, :3, :4, NVL(:5,'-'), :6)
                    """, rows)
                conn.commit()

        endTime = time.time()

        logging.info("EXTRACT 성공")
        logging.info(f'실행 시간: {round(endTime-startTime, 2)}초')
    except Exception as e:
        raise AirflowException(
            f"{upload_yutube_video_info.__name__} --> {e}"
            )


def upload_yutube_video_stats(table_name: str):
    logging.info("함수를 호출합니다. upload_yutube_video_stats()")

    try:
        startTime = time.time()

        # 비디오 정보를 가져온다.
        df = return_video_stats()

        with ADW_connection_cx_oracle() as conn:
            with conn.cursor() as cur:
                cur.fast_executemany = True

                rows = [tuple(x) for x in df.values]

                # bind 데이터 타입 확인 필수(특히 date형식)
                cur.executemany(
                    f"""MERGE INTO {table_name} a
                        USING DUAL
                        ON
                        (
                            a.VIDEO_ID = :1
                        AND a.BASEDATE = :2
                        )
                        WHEN MATCHED THEN
                            UPDATE SET
                                    a.VIEWCOUNT = :3
                                , a.LIKECOUNT = :4
                                , a.DISLIKECOUNT = :5
                                , a.FAVORITECOUNT = :6
                                , a.COMMENTCOUNT = :7
                        WHEN NOT MATCHED THEN
                        INSERT
                        (
                                a.VIDEO_ID
                            , a.BASEDATE
                            , a.VIEWCOUNT
                            , a.LIKECOUNT
                            , a.DISLIKECOUNT
                            , a.FAVORITECOUNT
                            , a.COMMENTCOUNT
                        )
                        VALUES (:1, :2, :3, :4, :5, :6, :7)
                    """, rows)
                conn.commit()

        endTime = time.time()

        logging.info("EXTRACT 성공")
        logging.info(f'실행 시간: {round(endTime-startTime, 2)}초')
    except Exception as e:
        raise AirflowException(
            f"{upload_yutube_video_stats.__name__} --> {e}"
            )


# main
# def youtube_upload_main():

#     # Database initialization
#     conn = ADW_connection_cx_oracle()

#     upload_yutube_channel_info(conn,
#                                return_channel_statistics(),
#                                'DW.youtube_channel_info')

#     upload_yutube_video_info(conn,
#                              return_channel_videos(),
#                              'DW.youtube_video_info')

#     # video list csv I/O 시간 고려
#     time.sleep(1)

#     upload_yutube_video_stats(conn,
#                               return_video_stats(),
#                               'DW.youtube_video_stats')

#     conn.close()
