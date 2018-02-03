select count(distinct lower(name)) 
from attorneys 
where name not like '%null%';
