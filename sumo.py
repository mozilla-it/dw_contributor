#!/usr/bin/python
import dw_mysql
import run_queries
import sys

lower_limit=sys.argv[1]
upper_limit=sys.argv[2]

# this is the max size of the "canonical" text field
# to avoid truncation warnings when the script is run
canonical_size=65000

def import_accounts(): 
  export_query="SELECT \
  email, '' as locale, '' as topic, '' as product,  \
  CONCAT('https://support.mozilla.org/en-US/user/',username) as canonical, \
  'Creating SUMO account' as action,  \
  0 as extra_id, date_joined as local_datetime, \
  TIMEDIFF(UTC_TIMESTAMP(),NOW()) as tz_offset \
  FROM auth_user \
  WHERE date_joined BETWEEN %s AND %s;"

  import_query="INSERT IGNORE INTO sumo_facts_raw \
  set email=%s, extra_locale=%s, extra_topic=%s,  \
  extra_product=%s, canonical=%s, action=%s,  \
  extra_id=%s, local_datetime=%s, tz_offset=%s;"

  dw_mysql.export_import("sumo", export_query, (str(lower_limit),str(upper_limit)), import_query)

def import_forum_posts(): 
  export_query="SELECT \
  email, locale, '' as topic, \
  IF(ISNULL(products_product.title),'',products_product.title) as product, \
  questions_answer.question_id as question,  \
  CONCAT('https://support.mozilla.org/questions/',questions_answer.question_id,'#answer-',questions_answer.id) as canonical, \
  'forum answer' as action, questions_answer.id as extra_id,  \
  questions_answer.updated as local_datetime, \
  TIMEDIFF(UTC_TIMESTAMP(),NOW()) as tz_offset \
  FROM questions_answer INNER JOIN auth_user ON (creator_id=auth_user.id) \
  INNER JOIN questions_question ON (question_id=questions_question.id) \
  LEFT JOIN products_product on (products_product.id=questions_question.product_id) \
  WHERE questions_answer.updated BETWEEN %s AND %s;"

  import_query="INSERT IGNORE INTO sumo_facts_raw \
  set email=%s, extra_locale=%s, extra_topic=%s,  \
  extra_product=%s, extra_question=%s, canonical=%s, action=%s,  \
  extra_id=%s, local_datetime=%s, tz_offset=%s;"

  dw_mysql.export_import("sumo", export_query, (str(lower_limit),str(upper_limit)), import_query)

def import_l10n():
  export_query="SELECT \
  email, 'localization' as action, locale as extra_locale, \
  CONCAT('https://support.mozilla.org/',locale,'/kb/',wiki_document.slug,'/revision/',wiki_revision.id) as canonical, \
  IF (ISNULL(products_product.title),'',products_product.title) as product, \
  IF (ISNULL(products_topic.title),'',products_topic.title) as topic, \
  wiki_document.id as extra_id, wiki_revision.created as local_datetime, \
  TIMEDIFF(UTC_TIMESTAMP(),NOW()) as tz_offset \
  FROM wiki_revision LEFT JOIN wiki_document on (document_id=wiki_document.id)  \
  LEFT JOIN auth_user ON (auth_user.id=creator_id) \
  LEFT JOIN wiki_document_products ON (wiki_document_products.document_id=wiki_document.id) \
  LEFT JOIN products_product on (products_product.id=wiki_document_products.product_id) \
  LEFT JOIN wiki_document_topics ON (wiki_document_topics.document_id=wiki_document.id) \
  LEFT JOIN products_topic on (products_topic.id=wiki_document_topics.topic_id) \
  WHERE locale!='en_US' AND wiki_revision.created BETWEEN %s AND %s;"  

  import_query="INSERT IGNORE INTO sumo_facts_raw \
  set email=%s, action=%s, extra_locale=%s,  \
  canonical=%s, extra_product=%s, extra_topic=%s,  \
  extra_id=%s, local_datetime=%s, tz_offset=%s;"
  
  dw_mysql.export_import("sumo", export_query, (str(lower_limit),str(upper_limit)), import_query)

def import_kb():
  export_query="SELECT \
  email, 'kb' as action, locale as extra_locale, \
  CONCAT('https://support.mozilla.org/',locale,'/kb/',wiki_document.slug,'/revision/',wiki_revision.id) as canonical, \
  IF (ISNULL(products_product.title),'',products_product.title) as product, \
  IF (ISNULL(products_topic.title),'',products_topic.title) as topic, \
  wiki_document.id as extra_id, wiki_revision.created as local_datetime, \
  TIMEDIFF(UTC_TIMESTAMP(),NOW()) as tz_offset \
  FROM wiki_revision LEFT JOIN wiki_document on (document_id=wiki_document.id)  \
  LEFT JOIN auth_user ON (auth_user.id=creator_id) \
  LEFT JOIN wiki_document_products ON (wiki_document_products.document_id=wiki_document.id) \
  LEFT JOIN products_product on (products_product.id=wiki_document_products.product_id) \
  LEFT JOIN wiki_document_topics ON (wiki_document_topics.document_id=wiki_document.id) \
  LEFT JOIN products_topic on (products_topic.id=wiki_document_topics.topic_id) \
  WHERE locale='en-US' AND wiki_revision.created BETWEEN %s AND %s;"

  import_query="INSERT IGNORE INTO sumo_facts_raw \
  set email=%s, action=%s, extra_locale=%s,  \
  canonical=%s, extra_product=%s, extra_topic=%s,  \
  extra_id=%s, local_datetime=%s, tz_offset=%s;"

  dw_mysql.export_import("sumo", export_query, (str(lower_limit),str(upper_limit)), import_query)

def import_contributors():
  dw_mysql.import_contributors_to_dimension('sumo',str(lower_limit),str(upper_limit))

def import_product():
  import_query="INSERT IGNORE INTO sumo_product (product_name) \
  SELECT distinct extra_product FROM sumo_facts_raw \
  WHERE local_datetime BETWEEN %s AND %s"
  run_queries.run_dw_query(import_query, (str(lower_limit),str(upper_limit)))
  
def import_topic():
  import_query="INSERT IGNORE INTO sumo_topic (topic_name) \
  SELECT distinct extra_topic FROM sumo_facts_raw \
  WHERE local_datetime BETWEEN %s AND %s"
  run_queries.run_dw_query(import_query, (str(lower_limit),str(upper_limit)))
  
def aggregate_to_sumo_facts():
  aggregate_query="INSERT IGNORE INTO sumo_facts ( \
  cnt, contributor_key, utc_datetime, canonical, \
  utc_date_key, product_key, topic_key, action, \
  locale, question, contribution_id_part) \
  SELECT 1, contributor_key,  \
  ADDTIME(local_datetime,tz_offset), \
  canonical, utc_date_key, product_key, topic_key, \
  action, extra_locale, extra_question, extra_id \
  FROM sumo_facts_raw INNER JOIN contributor USING (email) \
  INNER JOIN utc_date_only ON (DATE(ADDTIME(local_datetime,tz_offset))=utc_date_only) \
  INNER JOIN sumo_product ON (product_name=extra_product) \
  INNER JOIN sumo_topic ON (topic_name=extra_topic) \
  WHERE local_datetime BETWEEN %s AND %s;"
  run_queries.run_dw_query(aggregate_query, (str(lower_limit),str(upper_limit)))
  
def create_kb_revision_query(grp_cnt):
  kb_revision_query="INSERT IGNORE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key,  \
  conversion_key, source_key,team_key) \
  SELECT LEFT(group_concat(canonical)," + str(canonical_size) + ") as canonical, \
  max(utc_datetime), 1, utc_date_key, \
  contributor_key, for_n_edits.conversion_key,source.source_key,team.team_key \
  FROM contributor_facts \
  INNER JOIN conversion as for_1_edit ON (for_1_edit.conversion_desc='edit 1 article in KB' AND for_1_edit.conversion_key=contributor_facts.conversion_key) \
  INNER JOIN source ON (source_name='sumo') \
  INNER JOIN team ON (team_name='Sumo') \
  INNER JOIN conversion as for_n_edits ON (for_n_edits.conversion_desc='edit " + grp_cnt + " articles in kb') \
  WHERE utc_datetime BETWEEN %s - interval 1 year AND %s \
  GROUP BY contributor_key HAVING count(*)>=" + grp_cnt
  return kb_revision_query

def create_l10n_query(grp_cnt):
  l10n_query="INSERT IGNORE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key,  \
  conversion_key, source_key,team_key) \
  SELECT LEFT(group_concat(canonical)," + str(canonical_size) + ") as canonical, \
  max(utc_datetime), 1, utc_date_key, \
  contributor_key, for_n_l10ns.conversion_key,source.source_key,team.team_key \
  FROM contributor_facts \
  INNER JOIN conversion as for_1_l10n ON (for_1_l10n.conversion_desc='l10n 1 KB article' AND for_1_l10n.conversion_key=contributor_facts.conversion_key) \
  INNER JOIN source ON (source_name='sumo') \
  INNER JOIN team ON (team_name='Sumo') \
  INNER JOIN conversion as for_n_l10ns ON (for_n_l10ns.conversion_desc='l10n " + grp_cnt + " KB articles') \
  WHERE utc_datetime BETWEEN %s - interval 1 year AND %s \
  GROUP BY contributor_key HAVING count(*)>=" + grp_cnt
  return l10n_query

def create_forum_answer_query(grp_cnt):
  forum_answer_query="INSERT IGNORE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key,  \
  conversion_key, source_key,team_key) \
  SELECT LEFT(group_concat(canonical)," + str(canonical_size) + ") as canonical, \
  max(utc_datetime), 1, utc_date_key, \
  contributor_key, for_n_forum_answers.conversion_key,source.source_key,team.team_key \
  FROM sumo_facts \
  INNER JOIN source ON (source_name='sumo') \
  INNER JOIN team ON (team_name='Sumo') \
  INNER JOIN conversion as for_n_forum_answers ON (for_n_forum_answers.conversion_desc='" + grp_cnt + " answers on forum') \
  WHERE utc_datetime BETWEEN %s - interval 1 year AND %s \
  AND action='forum answer' \
  GROUP BY contributor_key HAVING count(*)>=" + grp_cnt
  return forum_answer_query


def aggregate_to_contributor_facts():
  create_account_query="REPLACE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key,  \
  conversion_key, source_key,team_key) \
  SELECT LEFT(group_concat(canonical)," + str(canonical_size) + ") as canonical, \
  min(utc_datetime), 1, min(utc_date_key),  \
  contributor_key, conversion_key, source_key, team_key \
  FROM sumo_facts  \
  INNER JOIN conversion ON (conversion_desc='Creating SUMO account') \
  INNER JOIN source ON (source_name='sumo') \
  INNER JOIN team ON (team_name='Sumo') \
  WHERE utc_datetime BETWEEN %s and %s \
  GROUP BY contributor_key"
  run_queries.run_dw_query(create_account_query, (str(lower_limit),str(upper_limit))) 

  edit_kb_article_query="INSERT IGNORE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key,  \
  conversion_key, source_key,team_key) \
  SELECT LEFT(group_concat(canonical)," + str(canonical_size) + ") as canonical, \
  utc_datetime, 1, utc_date_key, \
  contributor_key,conversion_key,source.source_key,team.team_key \
  FROM sumo_facts \
  INNER JOIN conversion ON (conversion_desc='edit 1 article in KB') \
  INNER JOIN source ON (source_name='sumo') \
  INNER JOIN team ON (team_name='Sumo') \
  WHERE utc_datetime BETWEEN %s and %s \
  AND action='kb';"
  run_queries.run_dw_query(edit_kb_article_query, (str(lower_limit),str(upper_limit)))

  localize_kb_query="INSERT IGNORE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key,  \
  conversion_key, source_key,team_key) \
  SELECT LEFT(group_concat(canonical)," + str(canonical_size) + ") as canonical, \
  utc_datetime, 1, utc_date_key, \
  contributor_key,conversion_key,source.source_key,team.team_key \
  FROM sumo_facts \
  INNER JOIN conversion ON (conversion_desc='l10n 1 KB article') \
  INNER JOIN source ON (source_name='sumo') \
  INNER JOIN team ON (team_name='Sumo') \
  WHERE utc_datetime BETWEEN %s and %s \
  AND action='localization';"
  run_queries.run_dw_query(localize_kb_query, (str(lower_limit),str(upper_limit)))

  edit_5_kb_articles_query=create_kb_revision_query(str(5))
  localize_5_kb_articles_query=create_l10n_query(str(5))
  at_least_4_forum_answers=create_forum_answer_query(str(4))
  at_least_10_forum_answers=create_forum_answer_query(str(10))
  # for each Monday from lower_limit to upper_limit
  # run the aggregate queries with the date as the param. 
  mondays=dw_mysql.get_mondays(str(lower_limit),str(upper_limit))
  for key,value in mondays.iteritems():
    for idx, val in enumerate(value):
      run_queries.run_dw_query(edit_5_kb_articles_query, (val,val))
      run_queries.run_dw_query(localize_5_kb_articles_query, (val,val))
      run_queries.run_dw_query(at_least_4_forum_answers, (val,val))
      run_queries.run_dw_query(at_least_10_forum_answers, (val,val))

def import_dates():
  dw_mysql.import_dates_to_UTC('sumo',str(lower_limit),str(upper_limit))

import_accounts()
import_forum_posts()
import_l10n()
import_kb()
import_contributors()
import_product()
import_topic()
import_dates()
aggregate_to_sumo_facts()
aggregate_to_contributor_facts()
