MERGE INTO DW.M_지역별환자발생 a
USING
    (  
        SELECT
                  A.병원명
                , A.기준월
                , A.입원외래검진구분
                , A.초재진구분
                , A.진료과
                , A.주소_시도
                , A.주소_시군구
                , SUM(A.ROWCOUNT) 환자수
                , (SELECT B.인구수
                     FROM DW.총인구조사 B
                    WHERE A.기준월 = B.기준월
                      AND A.주소_시도 = B.시도
                      AND A.주소_시군구 = B.시군구
                   ) 인구수
        FROM

        (
            SELECT
                      A.병원명
                    , A.수진기준일자
                    , TRUNC(A.수진기준일자,'MM') 기준월
                    , CASE WHEN A.최초내원종류 = '응급' AND A.입원외래검진구분 = '입원' THEN '외래'
                           ELSE A.입원외래검진구분
                      END 입원외래검진구분
                    , A.진료과
                    , A.초재진구분
                    , B.주소_시도
        --            , B.주소_시군구
                    , CASE WHEN
                                B.주소_시도 = '인천' AND
                                TRIM(CASE WHEN INSTR(B.주소_시군구,' ',1) > 0 THEN SUBSTR(B.주소_시군구, 1,INSTR(B.주소_시군구,' ',1))
                                    ELSE B.주소_시군구
                                END) = '남구' THEN '미추홀구'
                           ELSE
                               TRIM(CASE WHEN INSTR(B.주소_시군구,' ',1) > 0 THEN SUBSTR(B.주소_시군구, 1,INSTR(B.주소_시군구,' ',1))
                                    ELSE B.주소_시군구
                                END)
        
        
                      END 주소_시군구
                    , A.ROWCOUNT
        
            FROM DW.수진이력_전체 A
            LEFT OUTER JOIN DW.환자정보_전체 B ON A.DW환자번호 = B.DW환자번호
            WHERE 1 = 1
        --    AND EXTRACT (YEAR FROM A.수진기준일자) IN (2017, 2018, 2019,2020,2021)
            AND TRUNC(A.수진기준일자,'MM')  BETWEEN add_months(TRUNC(SYSDATE,'MM'), -2) AND add_months(TRUNC(SYSDATE,'MM'), -1)
            AND B.주소_시도 IS NOT NULL
        
        ) A
        GROUP BY A.병원명, A.기준월, A.입원외래검진구분, A.초재진구분, A.진료과, A.주소_시도, A.주소_시군구
    ) b
ON
        (
                  a.병원명 = b.병원명
             AND  a.기준월 = b.기준월
             AND  a.입원외래검진구분 = b.입원외래검진구분
             AND  a.초재진구분 = b.초재진구분
             AND  a.진료과 = b.진료과
             AND  a.주소_시도 = b.주소_시도
             AND  a.주소_시군구 = b.주소_시군구
         )

WHEN MATCHED THEN 
        UPDATE SET a.환자수 = b.환자수
                 , a.인구수 = b.인구수    


WHEN NOT MATCHED THEN 
        INSERT ( 
                    병원명
                  , 기준월
                  , 입원외래검진구분
                  , 초재진구분
                  , 진료과
                  , 주소_시도
                  , 주소_시군구
                  , 환자수
                  , 인구수
                )  

        VALUES (
                    b.병원명
                  , b.기준월
                  , b.입원외래검진구분
                  , b.초재진구분 
                  , b.진료과
                  , b.주소_시도
                  , b.주소_시군구
                  , b.환자수
                  , b.인구수
                )  