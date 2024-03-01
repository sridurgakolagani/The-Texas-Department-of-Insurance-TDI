SELECT * FROM TEXASDEPARTMENTOFINSURANCE.INSURANCECOMPLAINTSRAWDATA.INSURANCE_COMPLAINTS LIMIT 20;

SELECT COUNT(*) FROM TEXASDEPARTMENTOFINSURANCE.INSURANCECOMPLAINTSRAWDATA.INSURANCE_COMPLAINTS;


--Find the average number of complaints per month over the years:
SELECT TO_CHAR("received_date"::date, 'YYYY-MM') AS month,
       TO_CHAR("received_date"::date, 'YYYY') AS year,
       COUNT(*) AS num_complaints
FROM INSURANCE_COMPLAINTS
GROUP BY year, month
ORDER BY year, month;


--Identify the coverage type with the highest number of complaints:
SELECT "coverage_type", COUNT(*) AS num_complaints
FROM INSURANCE_COMPLAINTS
GROUP BY "coverage_type"
ORDER BY num_complaints DESC
LIMIT 1;


--Find the average time taken to close complaints for each coverage type:
SELECT "coverage_type", AVG(DATEDIFF('day', "received_date"::date, "closed_date"::date)) AS avg_days_to_close
FROM INSURANCE_COMPLAINTS
GROUP BY "coverage_type";


--Find the month with the highest number of complaints for each year:
SELECT year,
       month,
       num_complaints
FROM (
    SELECT TO_CHAR("received_date"::date, 'YYYY') AS year,
           TO_CHAR("received_date"::date, 'MM') AS month,
           COUNT(*) AS num_complaints,
           ROW_NUMBER() OVER (PARTITION BY TO_CHAR("received_date"::date, 'YYYY') ORDER BY COUNT(*) DESC) AS rank
    FROM INSURANCE_COMPLAINTS
    GROUP BY year, month
) AS ranked_months
WHERE rank = 1;


--Calculate the percentage of complaints confirmed for each coverage level:
SELECT "coverage_level", 
       COUNT(*) AS total_complaints,
       COUNT(CASE WHEN "complaint_confirmed_code" = 'Yes' THEN 1 END) AS confirmed_complaints,
       ROUND(100.0 * COUNT(CASE WHEN "complaint_confirmed_code" = 'Yes' THEN 1 END) / COUNT(*), 2) AS percentage_confirmed
FROM INSURANCE_COMPLAINTS
GROUP BY "coverage_level";


--Identify the top 3 reasons for complaints within each coverage type:
WITH RankedReasons AS (
    SELECT "coverage_type",
           "reason",
           COUNT(*) AS num_complaints,
           ROW_NUMBER() OVER (PARTITION BY "coverage_type" ORDER BY COUNT(*) DESC) AS rank
    FROM INSURANCE_COMPLAINTS
    GROUP BY "coverage_type", "reason"
)
SELECT "coverage_type",
       "reason",
       num_complaints
FROM RankedReasons
WHERE rank <= 3;
