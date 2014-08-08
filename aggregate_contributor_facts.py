#!/usr/bin/python
import run_queries
import dw_mysql
import sys 

lower_limit=sys.argv[1]
upper_limit=sys.argv[2]
print lower_limit
print upper_limit

import_active_contributors="INSERT IGNORE INTO contributor_active \
(contributor_key,c_date,team_name,source_name) \
SELECT contributor_key, %s, team_name, source_name \
FROM contributor_facts INNER JOIN conversion USING (conversion_key) \
INNER JOIN team USING (team_key) \
INNER JOIN source USING (source_key) \
WHERE contributor_level='active' and  \
utc_datetime BETWEEN %s - interval 1 year and %s"

update_new1="CREATE TABLE `contributor_new_%s` (contributor_key int unsigned not null,c_date date not null); "
update_new2="INSERT INTO `contributor_new_%s` (contributor_key,c_date)  \
select contributor_key,c_date from contributor_active  \
where c_date between %s - INTERVAL 1 YEAR and %s \
group by contributor_key \
having count(contributor_key) = 1 and c_date = %s"
update_new3="update contributor_active SET is_new=0 WHERE c_date=%s;"

update_new4="update contributor_active SET is_new=1 \
WHERE c_date=%s AND contributor_key in \
(  SELECT contributor_key FROM `contributor_new_%s` WHERE c_date=%s); "
update_new5="DROP TABLE `contributor_new_%s`;"

# for each Monday from lower_limit to upper_limit
# run the aggregate queries with the date as the param.
mondays=dw_mysql.get_mondays(str(lower_limit),str(upper_limit))
for key,value in mondays.iteritems():
  for idx, val in enumerate(value):
    run_queries.run_dw_query(import_active_contributors, (val,val,val))
    run_queries.run_dw_query(update_new1, (val))
    run_queries.run_dw_query(update_new2, (val,val,val,val))
    run_queries.run_dw_query(update_new3, (val))
    run_queries.run_dw_query(update_new4, (val,val,val))
    run_queries.run_dw_query(update_new5, (val))

