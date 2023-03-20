USE HEALTHCARETABLES;
-- Project Healthcare6 SQL grouping
-- PROBLEM 1
CREATE VIEW PERCENTAGE AS
(
SELECT PH.PHARMACYID, PH.PHARMACYNAME, ME.PRODUCTNAME, CO.QUANTITY, ME.HOSPITALEXCLUSIVE, YEAR(TR.DATE) "YEAR"
FROM PHARMACY PH INNER JOIN PRESCRIPTION PR ON PH.PHARMACYID = PR.PHARMACYID
INNER JOIN CONTAIN CO ON CO.PRESCRIPTIONID = PR.PRESCRIPTIONID
INNER JOIN MEDICINE ME ON ME.MEDICINEID = CO.MEDICINEID
INNER JOIN TREATMENT TR ON PR.TREATMENTID = TR.TREATMENTID WHERE YEAR(TR.DATE) = "2022"
);

SELECT PHARMACYID, PHARMACYNAME, @V1 := (SELECT SUM(QUANTITY) FROM PERCENTAGE WHERE PHARMACYID = P.PHARMACYID) "TOTAL QAUNTITY 2022",
@V2:= (SELECT SUM(QUANTITY) FROM PERCENTAGE WHERE PHARMACYID = P.PHARMACYID AND HOSPITALEXCLUSIVE = "S") "HE QUANTITY 2022",
@V2/@V1*100 "PERCENTAGE_2022" FROM PERCENTAGE P GROUP BY PHARMACYID ORDER BY PERCENTAGE_2022 DESC;

-- PROBLEM 2 # CHECK WITH VARIABLES
SELECT AD.STATE, COUNT(TR.TREATMENTID) "TREATMENT COUNT", COUNT(TR.CLAIMID) "CLAIM COUNT",  100 - COUNT(TR.CLAIMID)/COUNT(TR.TREATMENTID)*100 "PERCENTAGE" FROM ADDRESS AD INNER JOIN PERSON PE ON AD.ADDRESSID = PE.ADDRESSID
INNER JOIN TREATMENT TR ON TR.PATIENTID = PE.PERSONID GROUP BY AD.STATE; 

-- PROBLEM 3
CREATE VIEW DISEASESPREAD AS
(
SELECT AD.STATE, DI.DISEASENAME, COUNT(TR.TREATMENTID) "CNT" FROM ADDRESS AD INNER JOIN PERSON PE ON AD.ADDRESSID = PE.ADDRESSID
INNER JOIN TREATMENT TR ON TR.PATIENTID  = PE.PERSONID 
INNER JOIN DISEASE DI ON DI.DISEASEID = TR.DISEASEID WHERE YEAR(TR.DATE) = "2022" GROUP BY AD.STATE, DI.DISEASENAME ORDER BY 1
);
SELECT STATE, DISEASENAME, CNT FROM DISEASESPREAD DI WHERE CNT = (SELECT MAX(CNT) FROM DISEASESPREAD WHERE STATE = DI.STATE AND DISEASENAME = DI.DISEASENAME)
UNION 
SELECT STATE, DISEASENAME, CNT FROM DISEASESPREAD DI WHERE CNT = (SELECT MIN(CNT) FROM DISEASESPREAD WHERE STATE = DI.STATE AND DISEASENAME = DI.DISEASENAME);

-- PROBLEM 4
SELECT AD.CITY, COUNT(PA.PATIENTID) "PATIENT COUNT", COUNT(PA.PATIENTID)/(SELECT COUNT(*) FROM PATIENT)*100 "PERCENTAGE" FROM ADDRESS AD INNER JOIN PERSON PE ON PE.ADDRESSID = AD.ADDRESSID
INNER JOIN PATIENT PA ON PA.PATIENTID = PE.PERSONID GROUP BY CITY HAVING COUNT(PA.PATIENTID) >=10;

-- PROBLEM 5
SELECT COMPANYNAME, COUNT(MEDICINEID)"COUNT" FROM MEDICINE WHERE SUBSTANCENAME LIKE "%RANITIDINA%" GROUP BY COMPANYNAME ORDER BY 2 DESC LIMIT 3;
 