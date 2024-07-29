-- Exploratory Data Analysis 

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select * 
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

-- looking at the sum of the total_laid off filterd by company 

select company, sum(total_laid_off) 
from layoffs_staging2
group by company
order by 2 ;

-- looking at date range 

select min(`date`), max(`date`)
from layoffs_staging2;

-- looking at the %'s 

select company, avg(percentage_laid_off) 
from layoffs_staging2
group by company
order by 2 desc ;

-- looking at the rolling total of layoffs per month ---------------------------------------------

select substring(`date`, 1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where `date` is not null
group by `month`
order by 1 asc;
-- create a CTE and then use sum, over(order by) to get the rolling total of layoff. 
with rolling_total as 
(
select substring(`date`, 1,7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where `date` is not null
group by `month`
order by 1 asc
)
select `month`, total_off, sum(total_off) over(order by `month`) as rolling_total
from rolling_total;


-- lets look more into the companies ---------------------------------------------------------
-- Looking at layoff per year instead of a sum total. 

select company, sum(total_laid_off) 
from layoffs_staging2
group by company
order by 2 desc ;

select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company,  year(`date`)
order by company asc;

-- ranking the company by layoff total per year 

with company_year(company, years, total_laid_off) as 
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company,  year(`date`)
), Company_year_rank as
(
select * , dense_rank() over(partition by years order by total_laid_off desc) as Ranking
from company_year
where years is not null
)
select *
from Company_year_rank
where Ranking <=5;