with year_age(year,age) as (select strftime('%Y',filing_date) as year,
            strftime('%Y.%m%d',cases.filing_date) - strftime('%Y.%m%d',parties.dob) as age
        from (cases join charges using(case_id)) join parties using (case_id)
        where cases.filing_date is not '' and parties.name is not ''  and parties.dob is not ''
            and charges.disposition = 'Guilty' and parties.type = 'Defendant' and age between 0 and 100)
select year, avg(age)
from year_age
group by year
order by year desc
limit 5;
