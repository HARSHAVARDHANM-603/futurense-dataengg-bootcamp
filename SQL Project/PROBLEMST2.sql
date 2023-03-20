USE HEALTHCARETABLES;
-- Project Healthcare2 problem statements
-- PROBLEM 1
SELECT CITY, COUNT(PHARMACYID), COUNT(PHARMACYID)/SUM(CNT) "RATIO"
FROM (
SELECT AD.CITY, PR.PHARMACYID, COUNT(PR.PRESCRIPTIONID) "CNT" FROM PRESCRIPTION PR INNER JOIN PHARMACY PH ON PR.PHARMACYID = PH.PHARMACYID  INNER JOIN ADDRESS AD 
ON PH.ADDRESSID = AD.ADDRESSID GROUP BY PR.PHARMACYID
) A GROUP BY CITY HAVING SUM(CNT)>100 ORDER BY RATIO;

-- PROBLEM 2
SELECT CITY, DISEASEID, DISEASENAME, CNT
FROM( 
SELECT CITY, DISEASEID, DISEASENAME, CNT, ROW_NUMBER() OVER(PARTITION BY CITY ORDER BY CNT DESC) "ROW"
FROM( 
SELECT AD.CITY, TR.DISEASEID, DI.DISEASENAME , COUNT(PE.PERSONID) "CNT" FROM ADDRESS AD INNER JOIN PERSON PE ON AD.ADDRESSID = PE.ADDRESSID 
INNER JOIN TREATMENT TR ON PE.PERSONID = TR.PATIENTID
INNER JOIN DISEASE DI ON DI.DISEASEID = TR.DISEASEID
WHERE AD.STATE = "AL" GROUP BY AD.CITY, TR.DISEASEID, DI.DISEASENAME) A ORDER BY 1
) B WHERE B.ROW = 1;

-- PROBLEM 3
CREATE VIEW PROB023 AS
(
SELECT DI.DISEASENAME, IP.PLANNAME, COUNT(CL.CLAIMID) "CNT", DENSE_RANK() OVER(PARTITION BY DI.DISEASENAME ORDER BY COUNT(CL.CLAIMID)) "DENSERANK"
FROM INSURANCEPLAN IP INNER JOIN CLAIM CL ON IP.UIN = CL.UIN
INNER JOIN TREATMENT TR ON TR.CLAIMID = CL.CLAIMID
INNER JOIN DISEASE DI ON TR.DISEASEID = DI.DISEASEID
GROUP BY DI.DISEASENAME, IP.PLANNAME ORDER BY 1, 3
);
SELECT DISEASENAME, PLANNAME, CNT FROM PROB023 PRO WHERE DENSERANK IN (1, (SELECT MAX(DENSERANK) FROM PROB023 WHERE DISEASENAME = PRO.DISEASENAME));

-- PROBLEM 4
SELECT DISEASEID, DISEASENAME, COUNT(ADDRESSID) "ADDRESS COUNT"
FROM(
SELECT 	DI.DISEASENAME, TR.DISEASEID, AD.ADDRESSID, COUNT(PE.PERSONID) FROM ADDRESS AD INNER JOIN PERSON PE ON PE.ADDRESSID = AD.ADDRESSID
INNER JOIN TREATMENT TR ON TR.PATIENTID = PE.PERSONID
INNER JOIN DISEASE DI ON TR.DISEASEID = DI.DISEASEID 
GROUP BY TR.DISEASEID, AD.ADDRESSID HAVING(COUNT(PE.PERSONID))>1
) A GROUP BY DISEASEID, DISEASENAME ORDER BY 3 DESC;

-- PROBLEM 5
SELECT AD.STATE, COUNT(TR.TREATMENTID)/COUNT(TR.CLAIMID) "TR TO CLAIM" FROM ADDRESS AD INNER JOIN PERSON PE ON AD.ADDRESSID = PE.ADDRESSID
INNER JOIN TREATMENT TR ON TR.PATIENTID = PE.PERSONID WHERE TR.DATE BETWEEN "2021-04-01" AND "2022-03-31" GROUP BY AD.STATE;