select count(case_id) as cnt,substr(filing_date,0,4) || '0s' as year 
from cases
where filing_date is not '' 
group by year
order by cnt desc
limit 3;
