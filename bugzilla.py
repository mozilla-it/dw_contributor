#!/usr/bin/python
import dw_mysql
import run_queries
import sys 

lower_limit=sys.argv[1]
upper_limit=sys.argv[2]

def import_attachments(): 
  export_query="SELECT \
  attach_id,login_name,ispatch, \
  products.name,components.name, \
  modification_time, TIMEDIFF(UTC_TIMESTAMP(),NOW())  \
  FROM attachments \
  INNER JOIN profiles on (submitter_id=userid) \
  INNER JOIN bugs USING (bug_id) \
  INNER JOIN bugs.products ON (products.id=product_id) \
  INNER JOIN bugs.components ON (components.id=component_id) \
  WHERE modification_time BETWEEN %s AND %s;"

  import_query="INSERT IGNORE INTO bug_attachment \
  set attachment_key=%s, email=%s, ispatch=%s,  \
  product=%s, component=%s,  \
  local_datetime=%s, tz_offset=%s;"

  dw_mysql.export_import("bugzilla", export_query, (str(lower_limit),str(upper_limit)),import_query)

def import_bugs_activity():
  export_query= "SELECT \
  login_name,bug_when,TIMEDIFF(UTC_TIMESTAMP(),NOW()),  \
  fielddefs.name as field, bugs_activity.bug_id, added, removed, \
  attach_id, bug_status, products.name as product,  \
  components.name as component \
  FROM bugs.bugs_activity INNER JOIN bugs.bugs USING (bug_id) \
  INNER JOIN bugs.profiles ON (who=profiles.userid)INNER JOIN bugs.fielddefs ON (fieldid=fielddefs.id) \
  INNER JOIN bugs.products ON (products.id=product_id) \
  INNER JOIN bugs.components ON (components.id=component_id) \
  WHERE bug_when BETWEEN %s AND %s;" 

  import_query="INSERT IGNORE INTO bug_facts_raw \
  set contributor_email=%s, local_datetime=%s, tz_offset=%s, \
  fields=%s, bug_id=%s, added_values=%s, removed_values=%s, \
  attachment_id=%s, status=%s, product=%s, component=%s"

  dw_mysql.export_import("bugzilla", export_query, (str(lower_limit),str(upper_limit)),import_query)

def import_status():
  export_query="SELECT id,value from bug_status;"
  import_query="INSERT IGNORE INTO bug_status \
  (status_key,status_name) \
  VALUES (%s,%s);"
  dw_mysql.export_import("bugzilla", export_query, (),import_query)

def import_products():
  export_query="SELECT id,name from products;"
  import_query="INSERT IGNORE INTO bug_product \
  (product_key,product_name) \
  VALUES (%s,%s);"
  dw_mysql.export_import("bugzilla", export_query, (),import_query)

def import_components():
  export_query="SELECT id,name,product_id from components;"
  import_query="INSERT IGNORE INTO bug_component \
  (component_key,component_name,product_key) \
  VALUES (%s,%s,%s);"
  dw_mysql.export_import("bugzilla", export_query, (),import_query)

def import_comments():
  export_query="SELECT \
  login_name, longdescs.bug_when, \
  TIMEDIFF(UTC_TIMESTAMP(),NOW()) AS tz_offset,  \
  'comment', longdescs.bug_id, bug_status, \
  products.name, components.name \
  FROM bugs.longdescs INNER JOIN bugs.bugs USING (bug_id) \
  INNER JOIN bugs.profiles ON (who=profiles.userid) \
  INNER JOIN bugs.products ON (products.id=product_id) \
  INNER JOIN bugs.components ON (components.id=component_id) \
  WHERE bug_when BETWEEN %s and %s"
  import_query="INSERT IGNORE INTO bug_facts_raw \
  set contributor_email=%s, local_datetime=%s, tz_offset=%s, \
  fields=%s, bug_id=%s, status=%s, product=%s, component=%s"
  dw_mysql.export_import("bugzilla", export_query, (str(lower_limit),str(upper_limit)),import_query)

def number_comments():
  get_bugs_query="SELECT bug_id FROM bug_facts  \
  WHERE fields='comment' AND utc_datetime BETWEEN %s AND %s"
  bugs=run_queries.run_dw_query(get_bugs_query,(str(lower_limit),str(upper_limit)))

  for key,value in bugs.iteritems():
    for idx, val in enumerate(value):
      single_bug_query="SELECT min(utc_datetime) FROM bug_facts \
      WHERE fields='comment' AND bug_id=%s ORDER BY utc_datetime;"
      single_bug=run_queries.run_dw_query(single_bug_query,(val))
      for key2,value2 in single_bug.iteritems():
        for idx2, val2 in enumerate(value2):
          comment_num_update="UPDATE bug_facts SET comment_num=0 \
          WHERE bug_id=%s AND utc_datetime=%s"
          run_queries.run_dw_query(comment_num_update,(val,val2))
  
def import_account_creation():
  export_query="SELECT login_name, creation_ts, \
  TIMEDIFF(UTC_TIMESTAMP(),NOW()) AS tz_offset, \
  'Creating Bugzilla account', 0, '', '', '' \
  FROM bugs.profiles \
  WHERE creation_ts BETWEEN %s and %s"
  import_query="INSERT IGNORE INTO bug_facts_raw \
  set contributor_email=%s, local_datetime=%s, tz_offset=%s, \
  fields=%s, bug_id=%s, status=%s, product=%s, component=%s;"
  dw_mysql.export_import("bugzilla", export_query, (str(lower_limit),str(upper_limit)),import_query)

def populate_contributor():
  populate_contributor="INSERT IGNORE INTO contributor (email)  \
  SELECT contributor_email  \
  FROM bug_facts_raw  \
  WHERE local_datetime BETWEEN %s and %s";
  run_queries.run_dw_query(populate_contributor, (str(lower_limit),str(upper_limit)))

  populate_contributor="INSERT IGNORE INTO contributor (email)  \
  SELECT email  \
  FROM bug_attachment  \
  WHERE local_datetime BETWEEN %s and %s"
  run_queries.run_dw_query(populate_contributor, (str(lower_limit),str(upper_limit)))

def aggregate_to_bug_facts():
  aggregate_query="INSERT IGNORE INTO bug_facts (utc_datetime, fields, \
  canonical, added_values, removed_values, attachment_key, bug_id, \
  contributor_key,product_key, component_key, status_key, utc_date_key) \
  SELECT ADDTIME(local_datetime,tz_offset), fields,  \
  CONCAT('https://bugzilla.mozilla.org/show_bug.cgi?id=',bug_id) as canonical, \
  added_values, removed_values, attachment_id, bug_id, \
  contributor.contributor_key, bug_product.product_key, \
  IFNULL(bug_component.component_key,0),  \
  bug_status.status_key, utc_date_only.utc_date_key \
  FROM bug_facts_raw INNER JOIN contributor ON (contributor_email=email) \
  INNER JOIN utc_date_only ON (DATE(ADDTIME(local_datetime,tz_offset))=utc_date_only) \
  LEFT JOIN bug_status ON (status=status_name) \
  LEFT JOIN bug_product ON (product=product_name) \
  LEFT JOIN bug_component ON (component=component_name) \
  WHERE local_datetime BETWEEN %s AND %s;"
  run_queries.run_dw_query(aggregate_query, (str(lower_limit),str(upper_limit)))

def import_dates() :
  dw_mysql.import_dates_to_UTC('bugzilla',str(lower_limit),str(upper_limit))

def create_triage_query(grp_cnt):
  create_triage_query="INSERT IGNORE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key,  \
  conversion_key, source_key,team_key) \
  SELECT group_concat(canonical), max(utc_datetime), 1, utc_date_key, \
  contributor_key, for_n_triages.conversion_key,source_key,team_key \
  FROM contributor_facts \
  INNER JOIN conversion as for_1_triage ON (for_1_triage.conversion_desc in ('Change status 1 bug','Comment 1 bug','Change product 1 bug','Change component 1 bug') AND for_1_triage.conversion_key=contributor_facts.conversion_key) \
  INNER JOIN conversion as for_n_triages ON (for_n_triages.conversion_desc='Help triage " + grp_cnt + " bugs per quarter in bugzilla.mozilla.org') \
  WHERE utc_datetime BETWEEN %s - INTERVAL 1 QUARTER AND %s \
  GROUP BY contributor_key,for_1_triage.conversion_key,canonical HAVING count(*)>=" + grp_cnt
  return create_triage_query

def aggregate_to_contributor_facts():
  file_one_firefoxOS_bug_query="INSERT IGNORE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key, \
  conversion_key, source_key) \
  SELECT canonical, utc_datetime, 1, utc_date_key, \
  contributor_key, conversion_key, source_key \
  FROM bug_facts \
  INNER JOIN conversion ON (conversion_desc='Filing a bug Firefox OS') \
  INNER JOIN source ON (source_name='bugzilla') \
  LEFT JOIN bug_product USING (product_key) \
  LEFT JOIN team ON (team_name=product_name) \
  WHERE comment_num=0 AND product_name='Firefox OS' \
  AND utc_datetime BETWEEN %s and %s"
  run_queries.run_dw_query(file_one_firefoxOS_bug_query, (str(lower_limit),str(upper_limit)))

  create_bugzilla_account_query="INSERT IGNORE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key, \
  conversion_key, source_key) \
  SELECT canonical, utc_datetime, 1, utc_date_key, \
  contributor_key, conversion_key, source_key \
  FROM bug_facts \
  INNER JOIN conversion ON (conversion_desc='Creating Bugzilla account') \
  INNER JOIN source ON (source_name='bugzilla') \
  LEFT JOIN bug_product USING (product_key) \
  LEFT JOIN team ON (team_name=product_name) \
  WHERE fields='Creating Bugzilla account'  \
  AND utc_datetime BETWEEN %s AND %s"
  run_queries.run_dw_query(create_bugzilla_account_query, (str(lower_limit),str(upper_limit)))

  file_one_bug_query="INSERT IGNORE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key, \
  conversion_key, source_key) \
  SELECT canonical, utc_datetime, 1, utc_date_key, \
  contributor_key, conversion_key, source_key \
  FROM bug_facts \
  INNER JOIN conversion ON (conversion_desc='Filing a bug') \
  INNER JOIN source ON (source_name='bugzilla') \
  LEFT JOIN bug_product USING (product_key) \
  LEFT JOIN team ON (team_name=product_name) \
  WHERE comment_num=0 AND utc_datetime BETWEEN %s and %s"
  run_queries.run_dw_query(file_one_bug_query, (str(lower_limit),str(upper_limit)))

  submit_one_patch_query="INSERT IGNORE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key,  \
  conversion_key, source_key,team_key) \
  SELECT canonical, utc_datetime, 1, utc_date_key,  \
  contributor.contributor_key, conversion_key, source_key,IFNULL(team_key,0) \
  FROM bug_facts  \
  INNER JOIN contributor USING (contributor_key) \
  INNER JOIN conversion ON (conversion_desc='Submitting patch') \
  INNER JOIN source ON (source_name='bugzilla') \
  LEFT JOIN bug_product USING (product_key) \
  LEFT JOIN bug_attachment USING (attachment_key) \
  LEFT JOIN team ON (team_name=product_name) \
  WHERE ispatch=1 AND utc_datetime BETWEEN %s and %s"
  run_queries.run_dw_query(submit_one_patch_query, (str(lower_limit),str(upper_limit)))

  approve_one_patch_query="INSERT IGNORE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key, \
  conversion_key, source_key,team_key) \
  SELECT canonical, utc_datetime, 1, utc_date_key,  \
  contributor.contributor_key, conversion_key, source_key,IFNULL(team_key,0) \
  FROM bug_facts  \
  INNER JOIN conversion ON (conversion_desc='Having patch be approved') \
  INNER JOIN source ON (source_name='bugzilla') \
  INNER JOIN bug_attachment USING (attachment_key) \
  INNER JOIN contributor USING (contributor_key) \
  LEFT JOIN bug_product USING (product_key) \
  LEFT JOIN team ON (team_name=product_name) \
  WHERE fields = 'flagtypes.name' AND added_values REGEXP 'review\\+' \
  AND ispatch=1 AND utc_datetime BETWEEN %s AND %s;"
  run_queries.run_dw_query(approve_one_patch_query, (str(lower_limit),str(upper_limit)))

  one_triage_product_query="INSERT IGNORE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key, \
  conversion_key, source_key) \
  SELECT canonical, utc_datetime, 1, utc_date_key, \
  contributor_key, conversion_key, source_key \
  FROM bug_facts \
  INNER JOIN conversion ON (conversion_desc='Change product 1 bug') \
  INNER JOIN source ON (source_name='bugzilla') \
  LEFT JOIN bug_product USING (product_key) \
  LEFT JOIN team ON (team_name=product_name) \
  WHERE fields regexp 'product' AND utc_datetime BETWEEN %s and %s"
  run_queries.run_dw_query(one_triage_product_query, (str(lower_limit),str(upper_limit)))

  one_triage_component_query="INSERT IGNORE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key, \
  conversion_key, source_key) \
  SELECT canonical, utc_datetime, 1, utc_date_key, \
  contributor_key, conversion_key, source_key \
  FROM bug_facts \
  INNER JOIN conversion ON (conversion_desc='Change component 1 bug') \
  INNER JOIN source ON (source_name='bugzilla') \
  LEFT JOIN bug_product USING (product_key) \
  LEFT JOIN team ON (team_name=product_name) \
  WHERE fields regexp 'component' AND utc_datetime BETWEEN %s and %s"
  run_queries.run_dw_query(one_triage_component_query, (str(lower_limit),str(upper_limit)))

  one_triage_status_query="INSERT IGNORE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key, \
  conversion_key, source_key) \
  SELECT canonical, utc_datetime, 1, utc_date_key, \
  contributor_key, conversion_key, source_key \
  FROM bug_facts \
  INNER JOIN conversion ON (conversion_desc='Change status 1 bug') \
  INNER JOIN source ON (source_name='bugzilla') \
  LEFT JOIN bug_product USING (product_key) \
  LEFT JOIN team ON (team_name=product_name) \
  WHERE fields regexp 'bug_status' AND utc_datetime BETWEEN %s and %s"
  run_queries.run_dw_query(one_triage_status_query, (str(lower_limit),str(upper_limit)))

  one_triage_comment_query="INSERT IGNORE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key, \
  conversion_key, source_key) \
  SELECT canonical, utc_datetime, 1, utc_date_key, \
  contributor_key, conversion_key, source_key \
  FROM bug_facts \
  INNER JOIN conversion ON (conversion_desc='Comment 1 bug') \
  INNER JOIN source ON (source_name='bugzilla') \
  LEFT JOIN bug_product USING (product_key) \
  LEFT JOIN team ON (team_name=product_name) \
  WHERE fields='comment' AND utc_datetime BETWEEN %s and %s"
  run_queries.run_dw_query(one_triage_comment_query, (str(lower_limit),str(upper_limit)))

  triage_10_bugs_query=create_triage_query(str(10))
  triage_25_bugs_query=create_triage_query(str(25))
  triage_50_bugs_query=create_triage_query(str(50))
  triage_100_bugs_query=create_triage_query(str(100))

  # for each Monday from lower_limit to upper_limit
  # run the aggregate queries with the date as the param. 
  mondays=dw_mysql.get_mondays(str(lower_limit),str(upper_limit))
  for key,value in mondays.iteritems():
    for idx, val in enumerate(value):
      run_queries.run_dw_query(triage_10_bugs_query, (val,val))
      run_queries.run_dw_query(triage_25_bugs_query, (val,val))
      run_queries.run_dw_query(triage_50_bugs_query, (val,val))
      run_queries.run_dw_query(triage_100_bugs_query, (val,val))

import_products()
import_components()
import_status()
import_attachments()
import_bugs_activity()
import_comments()
import_account_creation()
import_dates()
populate_contributor()
aggregate_to_bug_facts()
number_comments()
aggregate_to_contributor_facts()

