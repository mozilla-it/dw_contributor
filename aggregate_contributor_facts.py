#!/usr/bin/python
import run_queries
import dw_mysql

lower_limit=sys.argv[1]
upper_limit=sys.argv[2]
print lower_limit
print upper_limit

def aggregate_contributor_to_active():
  import_active_contributors="INSERT IGNORE INTO contributor_active \
  (email,c_date,team_name,source_name) \
  SELECT email, %s, team_name, source_name \
  FROM contributor_facts INNER JOIN conversion USING (conversion_key) \
  INNER JOIN contributor USING (contributor_key) \
  INNER JOIN team USING (team_key) \
  INNER JOIN source USING (source_key) \
  WHERE contributor_level='active' and  \
  utc_datetime BETWEEN %s - interval 1 year and %s" 

  # for each Monday from lower_limit to upper_limit
  # run the aggregate queries with the date as the param. 
  mondays=dw_mysql.get_mondays(str(lower_limit),str(upper_limit))
  for key,value in mondays.iteritems():
    for idx, val in enumerate(value):
      run_queries.run_dw_query(import_active_contributors, (val,val,val))

aggregate_contributor_to_active()
