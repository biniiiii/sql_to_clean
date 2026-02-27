-- data cleaning
 
 select * from layoffs;
  
-- 1. check for duplicates,standardize data and fix errors,Look at null values etc

create table layoffs_staging select * from layoffs;
select * from layoffs_staging;

select *, row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as rownum from layoffs_staging;

with duplicate_cte as 
(select *,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as rownum 
from layoffs_staging)
select * from  duplicate_cte 
where rownum>1;

SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'casper';
 

with duplicate_cte as 
(select *,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as rownum 
from layoffs_staging)
delete from  duplicate_cte 
where rownum>1;

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
   `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2 select *,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as rownum 
from layoffs_staging;

SELECT * FROM layoffs_staging2 WHERE row_num>1;
delete FROM layoffs_staging2 WHERE row_num>1;

SET SQL_SAFE_UPDATES = 0;

alter table layoffs_staging2 drop row_num;

-- standardizind data (trim takes whitespaces off )

select company, trim(company) from layoffs_staging2;
update layoffs_staging2 set company=trim(company);

select distinct industry from layoffs_staging2 order by 1;

select * from layoffs_staging2 where industry like 'Crypto%';

update layoffs_staging2 set industry='Crypto' where industry like 'crypto%';

select * from layoffs_staging2 where industry='crypto currency';
select distinct industry from layoffs_staging2 order by 1;

select distinct location from layoffs_staging2 order by 1;
select distinct country from layoffs_staging2 order by 1;   -- united states and united states. are same 

select distinct country,trim(trailing '.' from country) from layoffs_staging2 order by 1;

update layoffs_staging2 set country=trim(trailing '.' from country) where country like 'United States%';
select `date` , str_to_date(`date`,'%m/%d/%Y') from layoffs_staging2;   -- converting fromat of date from text to date 

update layoffs_staging2 set date=str_to_date(`date`,'%m/%d/%Y');

select * from layoffs_staging2;

alter table layoffs_staging2 modify column `date`date;   -- changing datatype to date

-- working with null values 

select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;


select * from layoffs_staging2 where industry is null or industry='';
select * from layoffs_staging2 where company='Airbnb';

update layoffs_staging2 set industry=null where industry='';

select l1.company,l1.industry,l2.industry 
from layoffs_staging2 l1 join layoffs_staging2 l2 
on l1.company=l2.company 
where l1.industry is null 
and l2.industry is not null;

update layoffs_staging2 l1 
join layoffs_staging2 l2
on l1.company=l2.company 
set l1.industry=l2.industry
where l1.industry is null
and l2.industry is not null;

select * from layoffs_staging2;

delete from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;  -- no datas there to analyze


