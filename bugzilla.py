#!/usr/bin/python
import dw_mysql

lower_limit="2014-06-01 00:00:00";
upper_limit="2014-07-01 00:00:00";

#print lower_limit
#print upper_limit

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

import_attachments()
import_bugs_activity()

