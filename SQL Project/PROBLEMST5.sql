USE HEALTHCARETABLES;
-- Project Healthcare5 SQL grouping - 2
-- PROBLEM 1 
SELECT PE.PERSONNAME, TR.PATIENTID, COUNT(TR.TREATMENTID) "NUMBER OF TREATMENTS", TIMESTAMPDIFF(YEAR, PA.DOB, CURDATE()) "AGE" 
FROM PERSON PE INNER JOIN PATIENT PA ON PE.PERSONID = PA.PATIENTID 
INNER JOIN TREATMENT TR ON TR.PATIENTID = PA.PATIENTID GROUP BY PA.PATIENTID ORDER BY 4;

-- PROBLEM 2
SELECT DI.DISEASENAME, COUNT(TR.TREATMENTID) "TREATMENT COUNT", PE.GENDER FROM DISEASE DI INNER JOIN TREATMENT TR ON DI.DISEASEID = TR.DISEASEID
INNER JOIN PERSON PE ON PE.PERSONID = TR.PATIENTID WHERE YEAR(TR.DATE) = "2021" GROUP BY DI.DISEASENAME, PE.GENDER ORDER BY 1;

-- PROBLEM 3
SELECT DISEASENAME, CITY, CNT "TREATMENT COUNT" 
FROM (
SELECT DI.DISEASENAME, AD.CITY, COUNT(TR.TREATMENTID) "CNT", DENSE_RANK() OVER(PARTITION BY DI.DISEASENAME ORDER BY COUNT(TR.TREATMENTID) DESC) "DENSE"
FROM DISEASE DI INNER JOIN TREATMENT TR ON TR.DISEASEID = DI.DISEASEID
INNER JOIN PERSON PE ON PE.PERSONID = TR.PATIENTID
INNER JOIN ADDRESS AD ON AD.ADDRESSID = PE.ADDRESSID GROUP BY DI.DISEASENAME, AD.CITY
) A WHERE DENSE IN (1,2,3) GROUP BY DISEASENAME, CITY ORDER BY 1; 

-- PROBLEM 4	
CREATE VIEW BROOKEREQ AS
(
SELECT PH.PHARMACYNAME, DI.DISEASENAME, YEAR(TR.DATE) "YEAR", COUNT(PR.PRESCRIPTIONID) "CNT" FROM PHARMACY PH INNER JOIN PRESCRIPTION PR ON PH.PHARMACYID = PR.PHARMACYID
INNER JOIN TREATMENT TR ON TR.TREATMENTID  = PR.TREATMENTID 
INNER JOIN DISEASE DI ON DI.DISEASEID = TR.DISEASEID
WHERE YEAR(TR.DATE) IN ("2021", "2022")
GROUP BY PH.PHARMACYNAME, DI.DISEASENAME, YEAR(TR.DATE) ORDER BY 1
);

SELECT PHARMACYNAME, DISEASENAME, (SELECT CNT FROM BROOKEREQ WHERE YEAR = "2021" AND PHARMACYNAME = B.PHARMACYNAME AND DISEASENAME = B.DISEASENAME) "2021", 
(SELECT IFNULL(CNT,0) FROM BROOKEREQ WHERE YEAR = "2022" AND PHARMACYNAME = B.PHARMACYNAME AND DISEASENAME = B.DISEASENAME) "2022" FROM BROOKEREQ B GROUP BY PHARMACYNAME, DISEASENAME;

-- PROBLEM 5
SELECT IC.COMPANYNAME, AD.STATE, COUNT(TR.CLAIMID) FROM ADDRESS AD INNER JOIN INSURANCECOMPANY IC ON AD.ADDRESSID = IC.ADDRESSID
INNER JOIN INSURANCEPLAN IP ON IC.COMPANYID = IP.COMPANYID
INNER JOIN CLAIM CL ON IP.UIN = CL.UIN
INNER JOIN TREATMENT TR ON TR.CLAIMID = CL.CLAIMID GROUP BY IC.COMPANYNAME, AD.STATE; 