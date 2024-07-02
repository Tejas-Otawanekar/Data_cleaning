-- data cleaning
-- delete duplicate data

SELECT * FROM world_layoffs.layoffs;
set SQL_SAFE_UPDATE = 0;
use world_layoffs;

create table layoffs_staging
like layoffs;

insert layoffs_staging
select * from layoffs;

select *,
row_number() over(partition by  company, industry, total_laid_off , percentage_laid_off, 'date') 
from layoffs_staging;


with duplicate_cte as
(
select *,
row_number() over(partition by  company, location, industry, total_laid_off , percentage_laid_off, 'date', stage, country, funds_raised_millions ) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select * 
from layoffs_staging
where company = 'casper';


with duplicate_cte as
(
select *,
row_number() over(partition by  company, location, industry, total_laid_off , percentage_laid_off, 'date', stage, country, funds_raised_millions ) as row_num
from layoffs_staging
)
delete
from duplicate_cte
where row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * 
from layoffs_staging2
where row_num >1 ;

insert into layoffs_staging2
select *,
row_number() over(partition by  company, location, industry, total_laid_off ,
percentage_laid_off, 'date', stage, country, funds_raised_millions ) as row_num
from layoffs_staging;


delete  
from layoffs_staging2
where row_num >1 ;

select * 
from layoffs_staging2;



-- standardizing data

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- if we look at industry it looks like we have some null and empty rows, let's take a look at these


SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SET SQL_SAFE_UPDATES = 0;
UPDATE world_layoffs.layoffs_staging2
SET industry = 'crypto'
WHERE industry like 'crypto%';
SET SQL_SAFE_UPDATES = 1;


select * 
from layoffs_staging2
where country like 'united states%'
order by 1;


select distinct country, trim(TRAILING '.' FROM country)
from layoffs_staging2
order by 1;


UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);


select date,
str_to_date(date, '%m/%d/%y')
from layoffs_staging2;

update layoffs_staging2
set date = str_to_date(`date`,'%m/%d/%y');



-- we can use str to date to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM world_layoffs.layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


-- 4. remove any columns and rows we need to

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use


DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM world_layoffs.layoffs_staging2;























