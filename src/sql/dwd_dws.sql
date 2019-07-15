from
(select 
c.user_id,
c.type,
c.cnt,
c.content,
row_number() over(partition by c.user_id,c.type order by c.cnt) rn,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as dw_date
from
(select b.user_id as user_id,
b.type as type,
sum(b.pv) as cnt,
b.content as content
from
(select user_id,
split(types,':')[0] as type,
pv,
split(types,':')[1] as content
from
(select user_id,
concat('ip',':',visit_ip,',','cookie',':',cookie_id,',','浏览器',':',browser_name,',','操作系统',':',visit_os) as type,
pv
from qfbap_dwd.dwd_user_pc_pv) a lateral view explode(split(a.type,','))type as types) b
group by
b.user_id,
b.type,
b.content) c) d
insert overwrite table qfbap_dws.dws_user_visit_month1 partition(dt="20190710")
select *
;