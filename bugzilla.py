#!/usr/bin/python
import dw_mysql
import run_queries

lower_limit="1994-06-01 00:00:00";
upper_limit="2014-07-01 00:00:00";
print lower_limit
print upper_limit

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
  WHERE bug_when between %s and %s"
  import_query="INSERT IGNORE INTO bug_facts_raw \
  set contributor_email=%s, local_datetime=%s, tz_offset=%s, \
  fields=%s, bug_id=%s, status=%s, product=%s, component=%s"
  dw_mysql.export_import("bugzilla", export_query, (str(lower_limit),str(upper_limit)),import_query)

def import_account_creation():
  export_query="SELECT login_name, creation_ts, \
  TIMEDIFF(UTC_TIMESTAMP(),NOW()) AS tz_offset, \
  'Creating Bugzilla account', 0, '', '', '' \
  FROM bugs.profiles \
  WHERE creation_ts between %s and %s"
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


#import_products()
#import_components()
#import_status()
#import_attachments()
#import_bugs_activity()
#import_comments()
dw_mysql.import_dates_to_UTC('sumo',str(lower_limit),str(upper_limit))
#import_account_creation()
#populate_contributor()

