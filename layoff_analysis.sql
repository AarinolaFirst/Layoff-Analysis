--DATA CLEANING
--creating a duplicate from the main dataset
CREATE TABLE layoff_staging
LIKE layoffs;


---Duplicates dataset
SELECT * 
FROM layoff_staging

SELECT *
FROM layoff_staging
WHERE company is NULL OR
location is NULL OR
industry is NULL;

--replacing NULL with a specific value
UPDATE layoff_staging SET
location = 'Not Disclosed' WHERE
location IS NULL ;

UPDATE layoff_staging SET
industry = 'Not Disclosed' WHERE
industry IS NULL ;

UPDATE layoff_staging SET
total_laid_off = 0 WHERE
total_laid_off IS NULL ;

UPDATE layoff_staging SET
percentage_laid_off = 0 WHERE
percentage_laid_off IS NULL ;

UPDATE layoff_staging SET
funds_raised = 0 WHERE
funds_raised IS NULL ;

SELECT * 
FROM layoff_staging

Select *
FROM layoff_staging
WHERE company = 'oda' ;

--removing duplicates value USING ROW_NUMBER
WITH row_numbers AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY 
        company, 
        location, 
        industry, 
        total_laid_off, 
        percentage_laid_off, 
        date, 
        stage, 
        country, 
        funds_raised 
      ORDER BY 
        date
    ) AS row_num
  FROM 
    layoff_staging
)
DELETE FROM row_numbers
WHERE row_num > 1;

--Identifying inconsistent date format

SELECT DISTINCT 
  date,
  LEN(date) AS date_length,
  sql_variant_property(date, 'BaseType') AS data_type
FROM 
  layoff_staging;

SELECT * 
FROM layoff_staging

--Standardizing industry classification
SELECT DISTINCT industry
FROM layoff_staging;

UPDATE layoff_staging
SET industry = 
  CASE 
    WHEN industry IN ('AI', 'Data', 'Hardware', 'Crypto') THEN 'Technology'
    WHEN industry IN ('Legal', 'Marketing', 'HR', 'Recruiting', 'Support') THEN 'Professional Services'
    WHEN industry IN ('Healthcare', 'Fitness') THEN 'Healthcare'
    WHEN industry IN ('Finance', 'Crypto') THEN 'Finance'
    WHEN industry IN ('Retail', 'Consumer', 'Food') THEN 'Consumer Goods'
    WHEN industry IN ('Construction', 'Infrastructure', 'Real Estate', 'Energy') THEN 'Infrastructure'
    WHEN industry IN ('Transportation', 'Logistics') THEN 'Transportation'
    WHEN industry IN ('Media', 'Travel') THEN 'Media & Entertainment'
    WHEN industry = 'Education' THEN 'Education'
    WHEN industry = 'Security' THEN 'Security'
    WHEN industry = 'Manufacturing' THEN 'Manufacturing'
    WHEN industry = 'Aerospace' THEN 'Manufacturing'
    WHEN industry = 'Not Disclosed' THEN 'Other'
    ELSE 'Other'
  END;

SELECT * 
FROM layoff_staging

--converting total laid off and percentage laid  off to numerical data type
UPDATE layoff_staging
SET 
  total_laid_off = TRY_CONVERT(INT, total_laid_off),
  percentage_laid_off = TRY_CONVERT(DECIMAL(5,2), percentage_laid_off);

 SELECT DISTINCT country 
 FROM layoff_staging;


SELECT 
  COUNT(*) AS row_count,
  AVG(total_laid_off) AS avg_laid_off,
  MAX(total_laid_off) AS max_laid_off,
  MIN(total_laid_off) AS min_laid_off,
  STDEV(total_laid_off) AS std_dev_laid_off
FROM 
  layoff_staging;

--industry layoff by data data distribution
SELECT 
  industry,
  COUNT(*) AS count,
  AVG(total_laid_off) AS avg_laid_off
FROM 
  layoff_staging
GROUP BY 
  industry
ORDER BY 
  count DESC;

--missing values
SELECT *
FROM 
  layoff_staging
WHERE 
  total_laid_off IS NULL 
  OR industry IS NULL 
  OR country IS NULL;

--correlation analysis
WITH 
  laid_off_stats AS (
    SELECT 
      AVG(total_laid_off) AS avg_laid_off,
      AVG(percentage_laid_off) AS avg_percentage_laid_off,
      STDEV(total_laid_off) AS std_dev_laid_off,
      STDEV(percentage_laid_off) AS std_dev_percentage_laid_off
    FROM 
      layoff_staging
  ),
  
  covariance AS (
    SELECT 
      SUM((total_laid_off - (SELECT avg_laid_off FROM laid_off_stats)) * 
          (percentage_laid_off - (SELECT avg_percentage_laid_off FROM laid_off_stats))) / 
      (SELECT COUNT(*) FROM layoff_staging) AS covariance
    FROM 
      layoff_staging
  )
  
SELECT 
  c.covariance / (l.std_dev_laid_off * l.std_dev_percentage_laid_off) AS correlation_coefficient
FROM 
  laid_off_stats l,
  covariance c;
 
 --correlation analysis coefficient using variables or a single pass through the data.

DECLARE @avg_laid_off FLOAT, 
        @avg_percentage_laid_off FLOAT, 
        @std_dev_laid_off FLOAT, 
        @std_dev_percentage_laid_off FLOAT, 
        @count INT;

SELECT 
  @avg_laid_off = AVG(total_laid_off),
  @avg_percentage_laid_off = AVG(percentage_laid_off)
FROM 
  layoff_staging;

PRINT @avg_laid_off;
PRINT @avg_percentage_laid_off;

--top ten layoffs by industry
SELECT TOP 10 
  industry,
  SUM(total_laid_off) AS total_laid_off,
  AVG(percentage_laid_off) AS avg_percentage_laid_off
FROM 
  layoff_staging
GROUP BY 
  industry
ORDER BY 
  total_laid_off DESC;
