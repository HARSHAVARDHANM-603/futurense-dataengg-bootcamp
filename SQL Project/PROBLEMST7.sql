USE HEALTHCARETABLES;
# PROBLEM STATEMENT 7
# Project Healthcare7 SQL if-then-else
-- PROBLEM 1
DROP PROCEDURE IF EXISTS PROB071;
DELIMITER $$
CREATE PROCEDURE PROB071 (DISID INT)
BEGIN
DECLARE V1 INT;
DECLARE V2 INT;
SELECT AVG(CNT) "AVERAGE" INTO V1 FROM
(
SELECT DI.DISEASENAME, COUNT(TR.CLAIMID) "CNT" FROM TREATMENT TR INNER JOIN DISEASE DI ON DI.DISEASEID = TR.DISEASEID GROUP BY DI.DISEASENAME
) A;
SELECT COUNT(CLAIMID) INTO V2 FROM TREATMENT WHERE DISEASEID = DISID;
IF V2 > V1 THEN 
	SELECT "CLAIMED HIGHER THAN AVERAGE";
ELSE
	SELECT "CLAIMED LOWER THAN AVERAGE";
END IF;
END $$
DELIMITER ;
CALL PROB071(30);

-- PROBLEM 2
CREATE VIEW DISGEN AS
(
SELECT DI.DISEASENAME, DI.DISEASEID, TR.PATIENTID, PE.GENDER FROM PERSON PE INNER JOIN TREATMENT TR ON PE.PERSONID = TR.PATIENTID
INNER JOIN DISEASE DI ON TR.DISEASEID = DI.DISEASEID
);

DELIMITER $$
CREATE PROCEDURE GENREPDIS(DISID INT)
BEGIN
SELECT DISEASENAME , @V1 := (SELECT COUNT(PATIENTID) FROM DISGEN WHERE GENDER = "MALE" AND DISEASENAME = DI.DISEASENAME) "MALE NUM",
@V2 := (SELECT COUNT(PATIENTID) FROM DISGEN WHERE GENDER = "FEMALE" AND DISEASENAME = DI.DISEASENAME) "FEMALE NUM",
(CASE WHEN @V1=@V2 THEN "SAME"
	  WHEN @V1>@V2 THEN "MALE"
      WHEN @V1<@V2 THEN "FEMALE"
      END) "MORE TREATED" FROM DISGEN DI WHERE DISEASEID = DISID GROUP BY DISEASENAME;
END $$

CALL GENREPDIS(27);
-- PROBLEM 3 # DENSE RANK
CREATE VIEW TOP3 AS
(
SELECT IC.COMPANYNAME, IP.PLANNAME, COUNT(TR.CLAIMID) "CNT" FROM INSURANCECOMPANY IC INNER JOIN INSURANCEPLAN IP ON IC.COMPANYID = IP.COMPANYID
INNER JOIN CLAIM CL ON IP.UIN = CL.UIN
INNER JOIN TREATMENT TR ON TR.CLAIMID = CL.CLAIMID
GROUP BY IC.COMPANYNAME, IP.PLANNAME
);

(SELECT COMPANYNAME, PLANNAME, "MOSTCLAIMED", CNT FROM TOP3 ORDER BY CNT DESC LIMIT 3)
UNION
(SELECT COMPANYNAME, PLANNAME, "LEASTCLAIMED", CNT FROM TOP3 ORDER BY CNT ASC LIMIT 3);

-- PROBLEM 4
CREATE VIEW CATEPAT AS
(
SELECT DI.DISEASENAME, (CASE WHEN PA.DOB < "1970-01-01" AND PE.GENDER = "MALE" THEN "ELDERMALE"
							  WHEN PA.DOB < "1970-01-01" AND PE.GENDER = "FEMALE" THEN "ELDERFEMALE"
							  WHEN PA.DOB < "1985-01-01" AND PE.GENDER = "MALE" THEN "MIDAGEMALE"
							  WHEN PA.DOB < "1985-01-01" AND PE.GENDER = "FEMALE" THEN "MIDAGEFEMALE"
							  WHEN PA.DOB < "2005-01-01" AND PE.GENDER = "MALE" THEN "ADULTMALE"
							  WHEN PA.DOB < "2005-01-01" AND PE.GENDER = "FEMALE" THEN "ADULTFEMALE"
							  WHEN PA.DOB >= "2005-01-01" AND PE.GENDER = "MALE" THEN "YOUNGMALE"
							  WHEN PA.DOB >= "2005-01-01" AND PE.GENDER = "FEMALE" THEN "YOUNGFEMALE" 
							  END) "CATEGORY", COUNT(TR.PATIENTID) "CNT"  FROM PATIENT PA INNER JOIN PERSON PE ON PE.PERSONID = PA.PATIENTID
INNER JOIN TREATMENT TR ON TR.PATIENTID = PA.PATIENTID
INNER JOIN DISEASE DI ON DI.DISEASEID = TR.DISEASEID GROUP BY DI.DISEASENAME, CATEGORY ORDER BY 1, 2
);
SELECT DISEASENAME, CATEGORY, CNT FROM CATEPAT CP WHERE CNT = (SELECT MAX(CNT) FROM CATEPAT WHERE DISEASENAME = CP.DISEASENAME);

-- PROBLEM 5
SELECT * FROM 
(SELECT COMPANYNAME, PRODUCTNAME, DESCRIPTION, MAXPRICE, (CASE WHEN MAXPRICE>1000 THEN "PRICEY"
															  WHEN MAXPRICE<5 THEN "AFFORDABLE"
                                                              END) "CATEGORY" FROM MEDICINE ORDER BY MAXPRICE DESC) A WHERE CATEGORY IS NOT NULL;