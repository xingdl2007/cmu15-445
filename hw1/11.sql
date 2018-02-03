with att_dis(case_id, name, dis) as (
        select case_id, attorneys.name, charges.disposition 
        from charges join attorneys using (case_id)
        where attorneys.name is not ''),
    att_suc(name, cnt) as (
        select name, count(case_id) as cnt
        from att_dis
        where dis = 'Not Guilty'
        group by name)
select name, count(case_id) as num, cnt*100.0/count(case_id) as per
from att_dis natural join att_suc
group by name
having num > 100
order by per desc, num
limit 5;
