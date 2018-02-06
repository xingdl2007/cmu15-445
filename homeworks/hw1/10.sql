select zip, count(zip) cnt
from parties
where zip is not ''
group by zip
order by cnt desc
limit 3;
