/*
    M_진료협력
     DECODE(a.병원별기관코드, '201','부천','202','메디') = c.병원명
*/
-- CREATE OR REPLACE VIEW DW.M_진료협력 AS 


SELECT
          DECODE(a.병원별기관코드, '201','부천','202','메디') 병원명
        , a.DW환자번호
        , a.진료일자
        , a.진료과코드
        , (SELECT c.근무지부서 FROM DW.직원정보 c WHERE DECODE(a.병원별기관코드, '201','부천','202','메디') = c.병원명 AND a.진료과코드 = c.근무지부서코드 AND ROWNUM = 1) 진료과
        , a.진료의ID
        , (SELECT c.성명 FROM DW.직원정보 c WHERE a.진료의ID = c.사원번호 AND ROWNUM = 1) 진료의
        , b.병의원구분코드 AS 협력병원구분
        , a.협력병원코드
        , b.협력병의원명
        , a.협력의사코드
        , c.DRNM AS 협력의사명
        , b.지역구분
        , b.상위주소
        , a.협력의사시작일
        , CASE a.내원경로 WHEN 'E' THEN '응급'
                          WHEN 'O' THEN '외래'
                          WHEN 'I' THEN '입원'
                          ELSE NULL
          END 내원경로                
        
        , CASE a.초재진구분 WHEN 'F' THEN '병원초진'
                            WHEN 'R' THEN '재진'
                            WHEN 'S' THEN '상병초진(재초진)'
                            WHEN '5' THEN '입원경유'
                            WHEN '4' THEN '응급실경유(타과경유)'
                            WHEN 'D' THEN '과초진'
          END 초재진구분                  
        
        , CASE a.의뢰형태 WHEN '1' THEN '진료의뢰'
                          WHEN '2' THEN '검사의뢰'
                          WHEN '3' THEN '수술의뢰'
          END 외뢰형태                

        , CASE a.의뢰경로 WHEN '1' THEN '전화'
                          WHEN '2' THEN 'FAX'
                          WHEN '3' THEN 'E-Hospital'
                          WHEN '4' THEN '직접내원'
                          ELSE '기타'
          END 외뢰경로

        , CASE a.회신방법 WHEN '1' THEN 'FAX'
                          WHEN '2' THEN 'E-Hospital'   
                          WHEN '3' THEN '우편'
                          WHEN '4' THEN 'E-Mail'
                          ELSE '없음'
          END 회신방법
          
        , a.의뢰일자
        , a.완료일자
        
        , CASE a.회신유형 WHEN '1' THEN '해당없음'
                          WHEN '3' THEN '회신필'
                          WHEN '4' THEN '회신불필요'
          END 회신유형                
        
        , a.등록일자

        , CASE a.완료구분 WHEN '1' THEN '의뢰완료예정'
                          WHEN '2' THEN '되의뢰요청'
                          WHEN '3' THEN '회신서요청'
                          WHEN '4' THEN '완전완료'
         END 완료구분
         
        , a.진료정보공개동의여부

        , CASE a.완료형태 WHEN '1' THEN '일괄완료'
                          WHEN '2' THEN '전원완료'
                          WHEN '3' THEN '되의뢰완료'
          END 완료형태 
          
        , a.회신출력일시1차
        , a.입퇴원회신출력일시
        , a.삭제일자
        
        , CASE a.되의뢰유형 WHEN '1' THEN '되의뢰필'
                            WHEN '2' THEN '되의뢰불필요'
                            WHEN '3' THEN '해당무(누락)'
          END 되의뢰유형
          
        , a.입퇴원회신서작성자
        , a.추천구분
        , a.회신일자
        , 1 ROWCOUNT
        
FROM DW.의뢰이력 a
LEFT OUTER JOIN DW.협력병의원 b ON a.병원별기관코드 = b.병원별기관코드
                               AND a.협력병원코드 = b.협력병의원코드
LEFT OUTER JOIN DW.협력병의원의사 c ON a.병원별기관코드 = c.INSTCD
                                  AND a.협력의사코드 = c.COOPDRCD 


;

--GRANT SELECT ON DW.M_진료협력 TO dwu01;