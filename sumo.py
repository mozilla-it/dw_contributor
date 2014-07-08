#!/usr/bin/python
import dw_mysql
import run_queries

lower_limit="2014-06-01 00:00:00";
upper_limit="2014-07-01 00:00:00";

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
  LEFT JOIN questions_question_products ON (questions_question_products.question_id=questions_question.id) \
  LEFT JOIN products_product on (products_product.id=questions_question_products.product_id) \
  WHERE questions_answer.updated between %s and %s;"

  import_query="INSERT IGNORE INTO sumo_facts_raw \
  set email=%s, extra_locale=%s, extra_topic=%s,  \
  extra_product=%s, extra_question=%s, canonical=%s, action=%s,  \
  extra_id=%s, local_datetime=%s, tz_offset=%s;"

  dw_mysql.export_import("sumo", export_query, (str(lower_limit),str(upper_limit)), import_query)

#def WHAT_IS_THIS():
#  export_query="SELECT \
#  email, '' as product, '' as locale, slug as extra_topic,  \
#  CONCAT('https://support.mozilla.org/forums/contributors/',thread_id,'#post-',forums_post.id) as canonical, \
#  '????????????' as action, forums_post.id as extra_id,  \
#  forums_post.updated as local_datetime, \
#  TIMEDIFF(UTC_TIMESTAMP(),NOW()) as tz_offset \
#  FROM forums_post INNER JOIN auth_user ON (author_id=auth_user.id) \
#  INNER JOIN forums_thread ON (forums_post.thread_id=forums_thread.id) \
#  INNER JOIN forums_forum ON (forums_thread.forum_id=forums_forum.id) \
#  WHERE forums_post.updated between %s and %s;"
#
#  import_query="INSERT IGNORE INTO sumo_facts_raw \
#  set email=%s, extra_product=%s, extra_locale=%s,  \
#  extra_topic=%s, canonical=%s, action=%s,  \
#  extra_id=%s, local_datetime=%s, tz_offset=%s;"
#
#  dw_mysql.export_import("sumo", export_query, (str(lower_limit),str(upper_limit)), import_query)

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
  WHERE locale!='en_US' AND wiki_revision.created between %s and %s;"  

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
  WHERE locale='en-US' AND wiki_revision.created between %s and %s;"

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
  
def import_dates():
  dw_mysql.import_dates_to_UTC('sumo',str(lower_limit),str(upper_limit))

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
  
import_forum_posts()
import_l10n()
import_kb()
import_contributors()
import_product()
import_topic()
import_dates()
aggregate_to_sumo_facts()
