create table layoffs_staging
like layoffs; 

insert layoffs_staging 
select * from layoffs;

WITH duplicates_cte AS (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM world_layoffs.layoffs_staging
) 
select * from duplicates_cte;

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


-- created another table to use for deleting duplicates. Don't delete duplicates from the raw data. Make changes to a copied table of the raw data 
insert into layoffs_staging2
select *, 
row_number () over (
partition by company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
) as row_num
from layoffs_staging;

-- select statment to show the duplicates and to know what I am about to delete 
select * from layoffs_staging2
where row_num > 1;

-- delete the duplicates
delete 
from layoffs_staging2
where row_num > 1; 

-- check to make sure it worked with a select statment --
select * from layoffs_staging2
where row_num >1;

-- delete the duplicates ----------------------------------
delete 
from layoffs_staging2
where row_num >1;

-- check to see if the duplicates are gone -- 
select * from layoffs_staging2;

--  STANDARDIZING THE DATA ------------------------------------------------------------------------------------------------------------------------------------------------------

select company, (trim(company))
from layoffs_staging2;

--          we are going to update the layoffs_staging2 table by using the UPDATE key word. 
--          To get rid of the whitespace that can be on the ends of the values we can use TRIM. 
update layoffs_staging2
set company = trim(company); 

--        updated the industry column to make sure that there are not duplitcates and that the names in industry colunm match. 
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry like 'Crypto%';

--         use the below code to check if it worked. Also can use the bleow code to look at the industry colunm (or any column) to see if everything looks good and to identify any data that needs to be adjusted. 

select country
from layoffs_staging2
where country like 'United States%'
order by 1;

select *
from layoffs_staging2
where industry like 'Crypto%';

--            updated the country column since there are two versions of the 'United States'. This was found by using the above code to go through each column to look for any data that needed to be addressed. 

select *
from layoffs_staging2
where country like 'United States%'
order by 1;

-- 				Used this code to check to see if it would work before actually updating the table. 
select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1; 

--        		code to actually update the table. 
update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

--              Update the date column to be in date format. This will be helpul if a time series needs to be done on the data.  Format it so it is month/day/year. Can use str_to_date() to do this. 
update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');
-- 				This will change the data type of the date column to a date data type. Do not do this on your raw data table. ONLY do this on a copy of you raw data. 
alter table layoffs_staging2
modify column `date` date;


-- WORKING WITH NULL AND BLANK VALUES ----------------------------------------------------------------------------------------------------------------------------------------------

--               To look at the null values you can use 'IS NULL'. Also look for blank values as well. 
select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select * 
from layoffs_staging2
where industry is null
or industry = '';
--               Addressing the blank values. 
--               The code rigth below shows that Airbnb falls under the 'Travle' industry. 
select * 
from layoffs_staging2
where company like 'Bally%';

--                Note it can be a good idea to set blank values to null. This can make it easier to work with the data. 
-- ---------------Setting the blank values in the industry column to null. ------
update layoffs_staging2
set industry = null 
where industry = '';
-- -----------------------------


select t1.industry, t2.industry 
from layoffs_staging2 t1 
join layoffs_staging2 t2 
	on t1.company = t2.company 
where t1.industry is null
and t2.industry is not null;

-- this block of code will preform the actual update for the industry colunm.----
update layoffs_staging2 t1 
join layoffs_staging2 t2 
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;
-- ------------------------------------------------------




-- REMOVE COLUMNS AND ROWS THAT WE NEED TO ---------------------------------------------------------------------------------------------------------

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

select * 
from layoffs_staging2;
