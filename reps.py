#!/usr/bin/python
import dw_mysql
import run_queries
import sys 
import csv

lower_limit=sys.argv[1]
upper_limit=sys.argv[2]

def import_reps_raw():
  date_format="%Y/%M/%e"
  export_query="SELECT DISTINCT \
  auth_user.email, reports_ngreport.report_date, \
  reports_ngreport.created_on, TIMEDIFF(UTC_TIMESTAMP(),NOW()), \
  LEFT(reports_ngreport.link,255), \
  LEFT(IFNULL(reports_ngreport.activity_description,''),255), \
  IFNULL(reports_activity.name,''), IFNULL(reports_campaign.name,''), \
  IFNULL(profiles_functionalarea.name,''), reports_ngreport.location, \
  reports_ngreport.latitude, reports_ngreport.longitude,   \
  CONCAT('https://reps.mozilla.org/u/', profiles_userprofile.display_name, '/r/', \
  DATE_FORMAT(reports_ngreport.report_date, %s), \
  '/', reports_ngreport.id, '/') AS canonical \
  FROM reports_ngreport LEFT JOIN profiles_userprofile USING (mentor_id) \
  LEFT JOIN auth_user ON (profiles_userprofile.user_id=auth_user.id) \
  LEFT JOIN reports_activity ON (activity_id=reports_activity.id) \
  LEFT JOIN reports_campaign ON (campaign_id=reports_campaign.id) \
  LEFT JOIN events_event ON (event_id=events_event.id) \
  LEFT JOIN reports_ngreport_functional_areas ON (reports_ngreport.id=ngreport_id) \
  LEFT JOIN profiles_functionalarea ON (functionalarea_id=profiles_functionalarea.id) \
  WHERE auth_user.email IS NOT NULL \
  AND reports_ngreport.created_on BETWEEN %s AND %s;"

  import_query="INSERT IGNORE INTO reps_facts_raw \
  set email=%s, report_date=%s, local_datetime=%s, tz_offset=%s,  \
  event_url=%s, activity_desc=%s, activity=%s, campaign=%s, contribution_area=%s, \
  location=%s, latitude=%s, longitude=%s, canonical=%s;"

  dw_mysql.export_import("reps", export_query, (str(date_format),str(lower_limit),str(upper_limit)),import_query)

def populate_reps_activity():
  populate_reps_activity="INSERT IGNORE INTO reps_activity \
  (activity_name)  \
  SELECT distinct activity  \
  FROM reps_facts_raw  \
  WHERE local_datetime BETWEEN %s and %s";
  run_queries.run_dw_query(populate_reps_activity, (str(lower_limit),str(upper_limit)))

def populate_reps_location():
  populate_reps_location="INSERT IGNORE INTO reps_location \
  (location_name, longitude, latitude)  \
  SELECT distinct location, longitude, latitude  \
  FROM reps_facts_raw  \
  WHERE local_datetime BETWEEN %s and %s";
  run_queries.run_dw_query(populate_reps_location, (str(lower_limit),str(upper_limit)))

def import_campaign():
  export_query="SELECT id,name FROM reports_campaign;"
  import_query="INSERT IGNORE INTO reps_campaign \
  (campaign_key,campaign_name) \
  VALUES (%s,%s);"
  dw_mysql.export_import("reps", export_query, (),import_query)

def populate_contributor():
  populate_contributor="INSERT IGNORE INTO contributor (email)  \
  SELECT distinct email  \
  FROM reps_facts_raw  \
  WHERE local_datetime BETWEEN %s and %s";
  run_queries.run_dw_query(populate_contributor, (str(lower_limit),str(upper_limit)))

def import_dates():
  dw_mysql.import_dates_to_UTC('reps',str(lower_limit),str(upper_limit))

def aggregate_to_reps_facts():
  aggregate_query="INSERT IGNORE INTO reps_facts ( \
  contributor_key, canonical, action, reps_org_key, reps_repo_key, \
  utc_datetime, utc_date_key) \
  SELECT contributor.contributor_key, canonical, action, \
  IFNULL(reps_org.reps_org_key,0),  \
  IFNULL(reps_repo.reps_repo_key,0), \
  ADDTIME(local_datetime,tz_offset),  \
  utc_date_only.utc_date_key \
  FROM reps_facts_raw INNER JOIN contributor ON (reps_facts_raw.email=contributor.email) \
  INNER JOIN utc_date_only ON (DATE(ADDTIME(local_datetime,tz_offset))=utc_date_only) \
  LEFT JOIN reps_org ON (extra_reps_org=reps_org_name) \
  LEFT JOIN reps_repo ON (extra_reps_repo=reps_repo_name) \
  WHERE local_datetime BETWEEN %s AND %s;"
  run_queries.run_dw_query(aggregate_query, (str(lower_limit),str(upper_limit)))

def aggregate_to_contributor_facts():
  submit_patch_query="REPLACE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key,  \
  conversion_key, source_key,team_key) \
  SELECT canonical, utc_datetime, 1, utc_date_key,  \
  contributor_key, conversion_key, source_key, team_key \
  FROM reps_facts  \
  INNER JOIN conversion ON (conversion_desc='Submitting patch') \
  INNER JOIN source ON (source_name='reps') \
  INNER JOIN reps_repo ON (reps_facts.reps_repo_key=reps_repo.reps_repo_key) \
  WHERE utc_datetime BETWEEN %s and %s \
  AND action='pull-request-opened' "
  run_queries.run_dw_query(submit_patch_query, (str(lower_limit),str(upper_limit)))

  merge_patch_query="REPLACE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key,  \
  conversion_key, source_key,team_key) \
  SELECT canonical, utc_datetime, 1, utc_date_key,  \
  contributor_key, conversion_key, source_key, team_key \
  FROM reps_facts  \
  INNER JOIN conversion ON (conversion_desc='Having patch be merged') \
  INNER JOIN source ON (source_name='reps') \
  INNER JOIN reps_repo ON (reps_facts.reps_repo_key=reps_repo.reps_repo_key) \
  WHERE utc_datetime BETWEEN %s and %s \
  AND action='commit-author' "
  run_queries.run_dw_query(merge_patch_query, (str(lower_limit),str(upper_limit)))

import_reps_raw()
import_campaign()
populate_reps_activity()
populate_reps_location()
populate_contributor()
import_dates()
#aggregate_to_reps_facts()
#aggregate_to_contributor_facts()

