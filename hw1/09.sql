with race_dis (race,disposition) as (
        select parties.race, charges.disposition
        from parties join charges using (case_id)
        where parties.race in ('African American','Caucasian') 
            and charges.disposition in ('Guilty','Not Guilty')),
    race_cnt(race,cnt) as (
        select race, count(race)
        from race_dis
        group by race )
select race_dis.race, disposition, count(race_dis.race) * 100.0 / cnt 
from race_dis natural join race_cnt 
group by race_dis.race, disposition;
