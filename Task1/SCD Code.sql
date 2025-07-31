--Add Effective Date Columns:

ALTER TABLE Time_dim
ADD EFF_START_DATE DATE
 ADD   EFF_END_DATE DATE;

 MERGE INTO Time_dim dest
USING (SELECT DISTINCT EXTRACT(DAY FROM ADMISSION_DATE) AS DAY,
                             EXTRACT(MONTH FROM ADMISSION_DATE) AS MONTH,
                             EXTRACT(YEAR FROM ADMISSION_DATE) AS YEAR,
                             'NRY' AS DATASOURCE FROM NYR_ADMISSION) src
ON (dest.DAY = src.DAY AND dest.MONTH = src.MONTH AND dest.YEAR = src.YEAR AND dest.DATASOURCE = src.DATASOURCE)
WHEN MATCHED THEN UPDATE SET dest.EFF_END_DATE = SYSDATE
WHEN NOT MATCHED THEN INSERT (Time_Id, DAY, MONTH, YEAR, DATASOURCE, EFF_START_DATE, EFF_END_DATE)
   VALUES (Timeid_squence.NEXTVAL, src.DAY, src.MONTH, src.YEAR, 'NRY', SYSDATE, TO_DATE('9999-12-31', 'YYYY-MM-DD'));

MERGE INTO Time_dim dest
USING (
   SELECT DISTINCT EXTRACT(DAY FROM ADMISSION_DATE) AS DAY,
                    EXTRACT(MONTH FROM ADMISSION_DATE) AS MONTH,
                    EXTRACT(YEAR FROM ADMISSION_DATE) AS YEAR,
                    'WRY' AS DATASOURCE FROM WYR_RESERVATION
) src
ON (dest.DAY = src.DAY AND dest.MONTH = src.MONTH AND dest.YEAR = src.YEAR AND dest.DATASOURCE = src.DATASOURCE)
WHEN MATCHED THEN UPDATE SET dest.EFF_END_DATE = SYSDATE
WHEN NOT MATCHED THEN INSERT (
   Time_Id, DAY, MONTH, YEAR, DATASOURCE, EFF_START_DATE, EFF_END_DATE
) VALUES (
   Timeid_squence.NEXTVAL, src.DAY, src.MONTH, src.YEAR, 'WRY', SYSDATE, TO_DATE('9999-12-31', 'YYYY-MM-DD')
);

ALTER TABLE Care_Center
ADD EFF_START_DATE DATE
ADD    EFF_END_DATE DATE;

MERGE INTO Care_Center dest
USING (SELECT CARE_CENTRE_ID, CARE_CENTRE_NAME, 'NRY' AS DATASOURCE FROM NYR_CARE_CENTRE) src
ON (dest.CARE_ID = src.CARE_CENTRE_ID)
WHEN MATCHED THEN UPDATE SET dest.EFF_END_DATE = SYSDATE
WHEN NOT MATCHED THEN INSERT (CARE_ID, NAME, DATASOURCE, EFF_START_DATE, EFF_END_DATE)
   VALUES (CARE_CENTRE_ID, CARE_CENTRE_NAME, 'NRY', SYSDATE, TO_DATE('9999-12-31', 'YYYY-MM-DD'));


Select * from care_Center

MERGE INTO Care_Center dest
USING (
    SELECT CARE_ID, CARE_CENTRE_NAME, 'WRY' AS DATASOURCE 
    FROM WYR_CARE_CENTRE
) src
ON (dest.CARE_ID = src.CARE_ID)
WHEN MATCHED THEN 
    UPDATE SET dest.EFF_END_DATE = SYSDATE
WHEN NOT MATCHED THEN 
    INSERT (CARE_ID, NAME, DATASOURCE, EFF_START_DATE, EFF_END_DATE)
    VALUES (src.CARE_ID, src.CARE_CENTRE_NAME, 'WRY', SYSDATE, TO_DATE('9999-12-31', 'YYYY-MM-DD'));

-- Assuming you have a unique identifier for each row in your BedOccupency_Fact table, let's call it 'Serial_No'
-- Also assuming you have corresponding foreign keys in the BedOccupency_Fact table for Time, Ward, Care Center, and Bed

SELECT DISTINCT
    TO_CHAR(Time_dim.Year) AS "Year",
    TO_CHAR(Time_dim.Month) AS "Month",
    Ward.ward_Name AS "Ward",
    Care_Center.Name AS "Care Center",
    Bed_dim.Bed_Type AS "Bed Type",
    BedOccupency_Fact.TotalNo_Occupied_Beds AS "Occupied Beds"
FROM
    BedOccupency_Fact
JOIN
    Time_dim ON BedOccupency_Fact.Time_Id = Time_dim.Time_Id
JOIN
    Ward ON BedOccupency_Fact.Ward_ID = Ward.Ward_Id
JOIN
    Care_Center ON BedOccupency_Fact.Care_Id = Care_Center.Care_Id
JOIN
    Bed_dim ON BedOccupency_Fact.Bed_No = Bed_dim.Bed_No
WHERE
    BedOccupency_Fact.TotalNo_Occupied_Beds > 0;

DELETE FROM BedOccupency_Fact a
WHERE ROWID > (
    SELECT MIN(ROWID)
    FROM BedOccupency_Fact b
    WHERE a.Care_Id = b.Care_Id
);

DELETE FROM BedOccupency_Fact
WHERE ROWID NOT IN (
    SELECT MAX(ROWID)
    FROM BedOccupency_Fact
    GROUP BY Serial_No, Time_Id, Ward_ID, Care_Id, Bed_No, TotalNo_Occupied_Beds
);

select * from BedOccupency_Fact