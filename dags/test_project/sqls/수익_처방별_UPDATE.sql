MERGE INTO  dw.임시테이블 a
USING 
        (
            SELECT 
                      a.병원명
                    , a.수익마감일자
                    , a.입원외래검진구분
                    , SUM(a.수익_총수익) 수익_총수익
            
            FROM DW."수익_처방별_전체" a
            WHERE a.수익마감일자 between to_date(:1,'YYYYMMDD') and to_date(:2,'YYYYMMDD') 
            GROUP BY  a.병원명
                    , a.수익마감일자
                    , a.입원외래검진구분
        ) b
ON
        (
                  a.병원명 = b.병원명
             AND  a.수익마감일자 = b.수익마감일자
             AND  a.입원외래검진구분 = b.입원외래검진구분
         )
WHEN MATCHED THEN 
        UPDATE SET a.수익_총수익 = b.수익_총수익       

WHEN NOT MATCHED THEN 
        INSERT ( 
                    병원명
                  , 수익마감일자
                  , 입원외래검진구분
                  , 수익_총수익
                )  
        VALUES (
                    b.병원명
                  , b.수익마감일자
                  , b.입원외래검진구분
                  , b.수익_총수익
                )  