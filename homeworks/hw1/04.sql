select case_id,filing_date
from cases
where filing_date between '1950-00-00' and '1960-00-00'
order by filing_date
limit 3;
