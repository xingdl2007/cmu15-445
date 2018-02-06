select state, count(case_id) as cnt
from charges
where state is not '' and description like '%RECKLESS%'
group by state
order by cnt desc, state
limit 3;
