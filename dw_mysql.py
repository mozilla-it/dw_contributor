#!/usr/bin/python
import run_queries

def import_dates_to_UTC(source, lower_limit, upper_limit):
  if source=="sumo":
    table='sumo_facts_raw'
  if source=="github":
    table='github_facts_raw'
  if source=="bugzilla": 
    table='bug_facts_raw'

  import_query="INSERT IGNORE INTO utc_date_only (utc_date_only) \
  SELECT distinct(DATE(ADDTIME(local_datetime,tz_offset))) FROM " \
  + table + " WHERE local_datetime BETWEEN %s and %s ORDER BY 1;"
  run_queries.run_dw_query(import_query,(lower_limit,upper_limit))

def import_contributors_to_dimension (source, lower_limit, upper_limit):
  if source=="sumo":
    table='sumo_facts_raw'
  if source=="github":
    table='github_facts_raw'
  if source=="bugzilla": 
    table='bug_facts_raw'

  import_query="INSERT IGNORE INTO contributor (email) \
  SELECT DISTINCT email FROM " + table + \
  " WHERE local_datetime BETWEEN %s and %s;"
  run_queries.run_dw_query(import_query,(lower_limit,upper_limit))

def export_import (source, exp_query, exp_params, imp_query):
  if source=="sumo":
    output=run_queries.run_sumo_query(exp_query, exp_params)
  if source=="github":
    output=run_queries.run_github_query(exp_query, exp_params)
  if source=="bugzilla":
    output=run_queries.run_bugzilla_query(exp_query, exp_params)
  for key,value in output.iteritems():
    imp_params=()
    for idx, val in enumerate(value):
      imp_params=imp_params + (val,)
    #print imp_query ,imp_params
    run_queries.run_dw_query(imp_query, imp_params)
