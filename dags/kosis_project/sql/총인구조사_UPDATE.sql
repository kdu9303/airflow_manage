MERGE INTO  DW.총인구조사 a
USING 
        (
          SELECT 
                  a.기준월
                , a.지역코드
                , CASE a.시도 WHEN '강원도' THEN '강원'
                              WHEN '경기도' THEN '경기'
                              WHEN '경상남도' THEN '경남'
                              WHEN '경상북도' THEN '경북'
                              WHEN '광주광역시' THEN '광주'
                              WHEN '대구광역시' THEN '대구'
                              WHEN '대전광역시' THEN '대전'
                              WHEN '부산광역시' THEN '부산'
                              WHEN '서울특별시' THEN '서울'
                              WHEN '세종특별자치시' THEN '세종'
                              WHEN '울산광역시' THEN '울산'
                              WHEN '인천광역시' THEN '인천'
                              WHEN '전라남도' THEN '전남'
                              WHEN '전라북도' THEN '전북'
                              WHEN '제주특별자치도' THEN '제주'
                              WHEN '충청남도' THEN '충남'
                              WHEN '충청북도' THEN '충북'
                 END 시도
                
                , CASE  WHEN a.시도 = '인천광역시' AND a.시군구 = '남구' THEN '미추홀구'
                               ELSE a.시군구
                  END 시군구             
                , a.인구수
        
        FROM
            (
                    SELECT
                    
                              a.PRD_DE 기준월
                            , a.C1 지역코드
                            , (SELECT b.C1_NM
                                 FROM DW.CENSUS_POPULATION b
                                 WHERE LENGTH(b.C1) = 2
                                   AND SUBSTR(a.C1,1,2) = b.C1
                                   AND a.PRD_DE = b.PRD_DE
                               ) 시도
                            , a.C1_NM 시군구
                            , a.DT 인구수
                            
                    FROM DW.CENSUS_POPULATION a
                    WHERE LENGTH(a.c1) > 2
                    AND NOT (SUBSTR(a.c1,1,1) > 3 AND SUBSTR(a.c1,-1) <> 0)
                    AND a.C1_NM NOT LIKE '%출장소'
                    
                    -- 2개월 전 데이터부터 확인
                    and a.PRD_DE >= add_months(TRUNC(SYSDATE,'MM'), -2)
            ) a
        ) c         
ON
        (
                  a.기준월 = c.기준월
             AND  a.지역코드 = c.지역코드
             AND  a.시도 = c.시도
             AND  a.시군구 = c.시군구 
         )
WHEN MATCHED THEN 
        UPDATE SET a.인구수 = c.인구수       

WHEN NOT MATCHED THEN 
        INSERT ( 
                    기준월
                  , 지역코드
                  , 시도
                  , 시군구
                  , 인구수
                )  
                
        VALUES (
                    c.기준월
                  , c.지역코드
                  , c.시도
                  , c.시군구 
                  , c.인구수
                )  
