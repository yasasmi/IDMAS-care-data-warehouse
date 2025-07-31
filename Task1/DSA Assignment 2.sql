------------------------------------------------------------------------Creating the main tables------------------------------------------------------------------------
DROP TABLE BedOccupency_Fact CASCADE CONSTRAINTS;  

DROP TABLE Time_dim CASCADE CONSTRAINTS;

DROP TABLE Care_Center CASCADE CONSTRAINTS;

DROP TABLE Bed_dim CASCADE CONSTRAINTS;

DROP TABLE Ward CASCADE CONSTRAINTS;

DROP TABLE temp  CASCADE CONSTRAINTS;

DROP SEQUENCE Timeid_squence;

DROP SEQUENCE Serial_No_squence;

CREATE TABLE Time_dim(
	Time_Id	INTEGER,
	Day	INTEGER,
	Month INTEGER,
	Year INTEGER,
	CONSTRAINT pk_Time_dim PRIMARY KEY (Time_Id)
);


-- Create a Database table to represent the "Ward" entity.
CREATE TABLE Ward(
	Ward_Id	INTEGER NOT NULL,
	ward_Name VARCHAR(15),
	Ward_Capacity INTEGER,
	-- Specify the PRIMARY KEY constraint for table "Ward".
	CONSTRAINT pk_Ward PRIMARY KEY (Ward_Id)
);

-- Create a Database table to represent the "Care_Center" entity.
CREATE TABLE Care_Center(
	Care_Id	INTEGER NOT NULL,
	Name	VARCHAR(20),
	-- Specify the PRIMARY KEY constraint for table "Care_Center".
	CONSTRAINT	pk_Care_Center PRIMARY KEY (Care_Id)
);

-- Create a Database table to represent the "Bed" entity.
CREATE TABLE Bed_dim(
	Bed_No	INTEGER NOT NULL,
	Bed_Type	VARCHAR(25),
	Bed_Status	VARCHAR(15),
	-- Specify the PRIMARY KEY constraint for table "Bed".
	CONSTRAINT	pk_Bed PRIMARY KEY (Bed_No)
);

CREATE TABLE BedOccupency_Fact(
	Serial_No INTEGER , 
	Time_Id	INTEGER NOT NULL,
	Ward_ID	INTEGER NOT NULL,
	Care_Id	INTEGER NOT NULL,
	Bed_No	 INTEGER NOT NULL,
	TotalNo_Occupied_Beds	INTEGER,
    -- Specify the PRIMARY KEY constraint for table "BedOccupency_Fact_Table".
	CONSTRAINT	pk_BedOccupency_Table PRIMARY KEY (Time_Id,Ward_ID,Care_Id,Bed_No),
    -- Specify the FOREIGN KEY  constraint for table. 
    FOREIGN KEY (Time_Id) REFERENCES Time_dim(Time_Id),
    FOREIGN KEY (Ward_Id) REFERENCES Ward(Ward_Id),
    FOREIGN KEY (Care_Id) REFERENCES Care_Center(Care_Id),
    FOREIGN KEY (Bed_No) REFERENCES Bed_dim(Bed_No)

);

------------------------------------------------------------------------------------ETL---------------------------------------------------------------------------------------------------
------------------------------------------------------------------------explore the care_center table------------------------------------------------------------------------------------------------------
--DESC carecenter NRY
SELECT CARE_CENTRE_ID, CARE_CENTRE_NAME FROM NYR_CARE_CENTRE;

--DESC carecenter WRY;
SELECT CARE_ID, CARE_CENTRE_NAME FROM WYR_CARE_CENTRE;

-- I'm going to add a column to show where the data has come from
ALTER TABLE Care_Center 
ADD DATASOURCE VARCHAR2(5);

select * from Care_Center;

INSERT INTO  Care_Center (SELECT CARE_CENTRE_ID, CARE_CENTRE_NAME, 'NRY' FROM NYR_CARE_CENTRE);

-- Insert data from WYR_CARE_CENTRE with 'WRY' as DATASOURCE
INSERT INTO Care_Center (CARE_ID, NAME, DATASOURCE)
SELECT CARE_ID + (SELECT MAX(CARE_ID) FROM Care_Center), CARE_CENTRE_NAME, 'WRY'
FROM WYR_CARE_CENTRE
WHERE CARE_ID + (SELECT MAX(CARE_ID) FROM Care_Center) NOT IN (SELECT CARE_ID FROM Care_Center);



---------------------------------------------------------------------explore the Ward table---------------------------------------------------------------------------------------------------------

--DESC Ward NRY
SELECT WARD_ID, WARD_NAME, WARD_CAPACITY FROM NYR_WARD;

--DESC Ward WRY;
SELECT WARD_NO, WARD_NAME, WARD_CAPACITY FROM WYR_WARD;

ALTER TABLE Ward 
ADD DATASOURCE VARCHAR2(5);

INSERT INTO  Ward (SELECT WARD_ID, WARD_NAME,WARD_CAPACITY, 'NRY' FROM NYR_WARD);

select * from ward;

INSERT INTO Ward (WARD_ID, WARD_NAME, WARD_CAPACITY, DATASOURCE)
SELECT Ward_NO + (SELECT MAX(Ward_ID) FROM Ward), WARD_NAME,WARD_CAPACITY,'WRY'
FROM WYR_WARD
WHERE Ward_NO + (SELECT MAX(Ward_ID) FROM Ward) NOT IN (SELECT Ward_ID FROM Ward);

---------------------------------------------------------------------explore the Bed table---------------------------------------------------------------------------------------------------------
--DESC carecenter NRY
SELECT BED_ID, BED_TYPE, BED_STATUS FROM NYR_BED;

--DESC carecenter WRY;
SELECT BED_NO, BED_TYPE,BED_STATUS FROM WYR_BED;

ALTER TABLE Bed_dim 
ADD DATASOURCE VARCHAR2(5);

INSERT INTO  Bed_dim (SELECT BED_ID, BED_TYPE,BED_STATUS, 'NRY' FROM NYR_BED);

INSERT INTO Bed_dim (BED_NO, BED_TYPE, BED_STATUS, DATASOURCE)
SELECT BED_NO + (SELECT MAX(BED_NO) FROM Bed_dim), BED_TYPE,BED_STATUS,'WRY'
FROM WYR_BED
WHERE BED_NO + (SELECT MAX(BED_NO) FROM Bed_dim) NOT IN (SELECT BED_NO FROM Bed_dim);


---------------------------------------------------------------------explore the Time table---------------------------------------------------------------------------------------------------------
SELECT
    EXTRACT(DAY FROM ADMISSION_DATE) AS day,
    EXTRACT(MONTH FROM ADMISSION_DATE) AS month,
    EXTRACT(YEAR FROM ADMISSION_DATE) AS year
FROM NYR_ADMISSION;

SELECT
    EXTRACT(DAY FROM ADMISSION_DATE) AS day,
    EXTRACT(MONTH FROM ADMISSION_DATE) AS month,
    EXTRACT(YEAR FROM ADMISSION_DATE) AS year
FROM WYR_RESERVATION;

ALTER TABLE Time_dim 
ADD DATASOURCE VARCHAR2(5);

-- Creating a squence  for timeid Time_dim table
CREATE SEQUENCE Timeid_squence
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1;

INSERT INTO Time_dim (Time_id, day, month, Year, DATASOURCE)
SELECT 
    Timeid_squence.NEXTVAL, 
    EXTRACT(DAY FROM ADMISSION_DATE),
    EXTRACT(MONTH FROM ADMISSION_DATE),
    EXTRACT(YEAR FROM ADMISSION_DATE),
    'NRY'
FROM NYR_ADMISSION;


INSERT INTO Time_dim (Time_id, day, month, Year, DATASOURCE)
SELECT 
    Timeid_squence.NEXTVAL, 
    EXTRACT(DAY FROM ADMISSION_DATE),
    EXTRACT(MONTH FROM ADMISSION_DATE),
    EXTRACT(YEAR FROM ADMISSION_DATE),
    'WRY'
FROM WYR_RESERVATION;

------------------------------------------------------------------------explore the BedOccupency_Fact table------------------------------------------------------------------------------------------------------



-- calculating the TotalNo_Occupied_Beds 
/*
SELECT
    WYR_Ward.WARD_NO,
    WYR_Ward.CARE_ID,
    COUNT(WYR_BED.BED_NO) AS Total_Occupied_Beds
FROM
    WYR_CARE_CENTRE
JOIN
    WYR_Ward ON WYR_CARE_CENTRE.CARE_ID = WYR_Ward.CARE_ID
JOIN
    WYR_BED ON WYR_Ward.WARD_NO = WYR_BED.WARD_NO
WHERE
    WYR_BED.BED_STATUS = 'Occupied'
GROUP BY
    WYR_Ward.WARD_NO;


SELECT
    NYR_WARD.WARD_ID,
    NYR_WARD.CARE_CENTRE_ID,
    COUNT(NYR_BED.BED_ID) AS Total_Occupied_Beds
FROM
    NYR_CARE_CENTRE
JOIN
    NYR_WARD ON NYR_CARE_CENTRE.CARE_CENTRE_ID = NYR_CARE_CENTRE.CARE_CENTRE_ID
JOIN
    NYR_BED ON NYR_Ward.WARD_ID = NYR_BED.WARD_ID
WHERE
    NYR_BED.BED_STATUS = 'OCCUPIED'
GROUP BY
     NYR_Ward.CARE_CENTRE_ID,NYR_Ward.WARD_ID;
*/
CREATE TABLE temp (
    Ward_ID INT  Primary key,
    Care_Id INT,
    Total_Occupied_Beds INT
);



 -- Insert data from WYR_Ward, WYR_CARE_CENTRE, and WYR_BED into temp table
INSERT INTO temp (WARD_ID, CARE_ID, Total_Occupied_Beds)
SELECT
    WYR_Ward.WARD_NO,
    WYR_Ward.CARE_ID,
    COUNT(WYR_BED.BED_NO) AS Total_Occupied_Beds
FROM
    WYR_CARE_CENTRE
JOIN
    WYR_Ward ON WYR_CARE_CENTRE.CARE_ID = WYR_Ward.CARE_ID
JOIN
    WYR_BED ON WYR_Ward.WARD_NO = WYR_BED.WARD_NO
WHERE
    WYR_BED.BED_STATUS = 'Occupied'
GROUP BY
    WYR_Ward.CARE_ID, WYR_Ward.WARD_NO;

-- Insert data from NYR_WARD, NYR_CARE_CENTRE, and NYR_BED into temp table
INSERT INTO temp (WARD_ID, CARE_ID, Total_Occupied_Beds)
SELECT
    NYR_WARD.WARD_ID + (SELECT MAX(Ward_ID) FROM temp),
    NYR_CARE_CENTRE.CARE_CENTRE_ID,
    COUNT(NYR_BED.BED_ID) AS Total_Occupied_Beds
FROM 
    NYR_CARE_CENTRE
JOIN
    NYR_WARD ON NYR_CARE_CENTRE.CARE_CENTRE_ID = NYR_WARD.CARE_CENTRE_ID
JOIN
    NYR_BED ON NYR_WARD.WARD_ID = NYR_BED.WARD_ID
WHERE
    NYR_BED.BED_STATUS = 'OCCUPIED'
    AND NYR_WARD.WARD_ID + (SELECT MAX(Ward_ID) FROM temp) NOT IN (SELECT Ward_ID FROM temp)
GROUP BY
    NYR_WARD.CARE_CENTRE_ID, NYR_WARD.WARD_ID, NYR_CARE_CENTRE.CARE_CENTRE_ID;


-- creating a sequence for Serial_No BedOccupency_Fact Table

CREATE SEQUENCE Serial_No_squence
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1;

-- Insert data into BedOccupency_Fact table
INSERT INTO BedOccupency_Fact (Serial_No, Time_Id, Ward_ID, Care_Id, Bed_No, TotalNo_Occupied_Beds)
SELECT 
    Serial_No_squence.NEXTVAL,
    t.Time_Id,
    temp.Ward_ID,
    temp.Care_Id,
    b.Bed_No,
    temp.Total_Occupied_Beds
FROM 
    Time_dim t,temp, Bed_dim b;



----------------------------------------------Clean the data-----------------------------------------------

-- Replace "GENERAL WARD" with "GENERAL CARE"
UPDATE WARD
SET WARD_NAME = 'GENERAL CARE'
WHERE WARD_NAME = 'GENERAL WARD';