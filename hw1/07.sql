select name,count(distinct state) as cnt
from parties
where name is not '' and type = 'Defendant'
group by name
order by cnt desc
limit 3;
