#!/usr/bin/python
import dw_mysql
import run_queries
import sys 
import csv

lower_limit=sys.argv[1]
upper_limit=sys.argv[2]

def import_github_activity():
  export_query="SELECT \
  github_public_email,github_commit_url,happened_on, \
  github_organization,github_repository, \
  IFNULL(github_username,github_public_email),LEFT(commit_msg,255), \
  commit_id,action_type \
  FROM gitribution2 \
  WHERE github_public_email is not null AND happened_on BETWEEN %s AND %s;"

  import_query="INSERT IGNORE INTO github_facts_raw \
  set email=%s, canonical=%s, local_datetime=%s,  \
  extra_github_org=%s, extra_github_repo=%s,  \
  extra_github_username=%s, extra_commit_msg=%s,  \
  extra_commit_id=%s, action=%s;"

  dw_mysql.export_import("github", export_query, (str(lower_limit),str(upper_limit),str(lower_limit),str(upper_limit)),import_query)

def populate_github_org():
  populate_github_org="INSERT IGNORE INTO github_org \
  (github_org_name)  \
  SELECT distinct extra_github_org  \
  FROM github_facts_raw  \
  WHERE local_datetime BETWEEN %s and %s";
  run_queries.run_dw_query(populate_github_org, (str(lower_limit),str(upper_limit)))

def populate_github_repo():
  populate_github_repo="INSERT IGNORE INTO github_repo \
  (github_repo_name)  \
  SELECT distinct extra_github_repo  \
  FROM github_facts_raw  \
  WHERE local_datetime BETWEEN %s and %s";
  run_queries.run_dw_query(populate_github_repo, (str(lower_limit),str(upper_limit)))

#see https://docs.google.com/spreadsheets/d/1cMZQYEmEiuDTAY93j4siiq58exaMZOa2o3YdoLyEYTI/edit#gid=0
  set_repo_teams="UPDATE github_repo LEFT JOIN team ON \
  (team.team_name=%s) \
  SET github_repo.team_key=team.team_key \
  WHERE github_repo_name in (%s);"
  list_of_repo_teams=(('Firefox OS', '123done'),('Firefox OS', 'acidity'),('Firefox','gecko-dev'),('Firefox OS', 'gaia'),('Firefox OS', 'b2g'),('Firefox OS', 'b2g-manifests'), ('Firefox OS', 'gonk-misc'), ('Firefox OS', 'fxos-certsuite'), ('Firefox OS', 'device-flame'), ('Firefox OS', 'marionette-js-runner'), ('Firefox OS', 'device-gp-keon'), ('Firefox OS', 'screencap-gonk'), ('Firefox OS', 'orangutan'), ('Firefox OS', 'android-device-crespo4g'), ('Firefox OS', 'unbootimg'), ('Firefox OS', 'fake-dalvik'), ('Firefox OS', 'fake-libdvm'), \
  ('Firefox OS', 'gonk-patches'), ('Firefox OS', 'platform_external_apriori'), ('Firefox OS', 'moztt'), ('Firefox OS', 'device-wasabi'), ('Firefox OS', 'platform_prebuilts_qemu-kernel'), ('Firefox OS', 'librecovery'), ('Firefox OS', 'device-fugu'), ('Firefox OS', 'device-leo'), ('Firefox OS', 'android-device-unagi'), ('Firefox OS', 'android-device-panda'), ('Firefox OS', 'device-helix'), ('Firefox OS', 'android-device-hamachi'), ('Firefox OS', 'device_generic_goldfish'), ('Firefox OS', 'device-flatfish'), ('Firefox OS', 'device-inari'), ('Firefox OS', 'notes'), ('Firefox OS', 'sockit-to-me'), ('Firefox OS', 'gaia-node-modules'), \
  ('Firefox OS', 'fxos-appgen'), ('Firefox OS', 'gaia-email-libs-and-more'), ('Firefox OS', 'mocha-tbpl-reporter'), ('Firefox OS', 'marionette-b2gdesktop-host'), ('Firefox OS', 'marionette-extension'), ('Firefox OS', 'gaia-specs'), ('Firefox OS', 'kernel_goldfish'), ('Firefox OS', 'platform_system_nfcd'), ('Firefox OS', 'marionette-apps'), ('Firefox OS', 'js-test-agent'), ('Firefox OS', 'firefoxos-loop-client'), ('Firefox OS', 'travis-project-jobs'), ('Firefox OS', 'mail-fakeservers'), ('Firefox OS', 'marionette-plugin-forms'), ('Firefox OS', 'marionette-js-client'), ('Firefox OS', 'marionette-js-logger'), ('Firefox OS', 'caldav'), ('Firefox OS', 'mozilla-runner'), ('Firefox OS', 'uplift'), ('Firefox OS', 'marionette-helper'), ('Firefox OS', 'Gaia-UI-Building-Blocks'), ('Firefox OS', 'jsas'), ('Firefox OS', 'jswbxml'), ('Firefox OS', 'bisect_b2g'), ('Firefox OS', 'marionette-content-script'), ('Firefox OS', 'device-sora'), ('Firefox OS', 'marionette-device-host'), \
  ('Firefox OS', 'marionette-profile-builder'), ('Firefox OS', 'mozilla-profile-builder'), ('Firefox OS', 'ntpclient-android'), ('Firefox OS', 'marionette-file-manager'), ('Firefox OS', 'mozilla-get-url'), ('Firefox OS', 'mozilla-download'), ('Firefox OS', 'mocha-json-proxy'), \
  ('Firefox OS', 'marionette-firefox-host'), ('Firefox OS', 'marionette-debug'), ('Firefox OS', 'marionette-orientation'), ('Firefox OS', 'mozilla-extract'), ('Firefox OS', 'mozilla-detect-os'), ('Firefox OS', 'bleach.js'), ('Firefox OS', 'marionette-debug-server'), ('Firefox OS', 'marionette-settings-api'), ('Firefox OS', 'haida-planning'), ('Firefox OS', 'gaia-profile-builder'), ('Firefox OS', 'boing'), ('Firefox OS', 'dogdish'), ('Firefox OS', 'dogfood-setup'), ('Firefox OS', 'gaia-botio-scripts'), ('Firefox OS', 'b2g-toolchains'), \
('QA', 'mozilla-services'), ('QA', 'mozilla/fxa-auth-server'), ('QA', 'mozilla/fxa-content-server'), ('QA', 'mozilla/fxa-js-client'), ('QA', 'mozilla-services/pushgo'), ('QA', 'mozilla-services/puppet-config'), ('QA', 'mozilla-services/svcops-oompaloompas'))

  for team_name, repo_name in list_of_repo_teams:
    run_queries.run_dw_query(set_repo_teams, (str(team_name),str(repo_name)))

def populate_contributor():
  populate_contributor="INSERT IGNORE INTO contributor (email)  \
  SELECT distinct email  \
  FROM github_facts_raw  \
  WHERE local_datetime BETWEEN %s and %s";
  run_queries.run_dw_query(populate_contributor, (str(lower_limit),str(upper_limit)))

def aggregate_to_github_facts():
  aggregate_query="INSERT IGNORE INTO github_facts ( \
  contributor_key, canonical, action, github_org_key, github_repo_key, \
  utc_datetime, utc_date_key) \
  SELECT contributor.contributor_key, canonical, action, \
  IFNULL(github_org.github_org_key,0),  \
  IFNULL(github_repo.github_repo_key,0), \
  ADDTIME(local_datetime,tz_offset),  \
  utc_date_only.utc_date_key \
  FROM github_facts_raw INNER JOIN contributor ON (github_facts_raw.email=contributor.email) \
  INNER JOIN utc_date_only ON (DATE(ADDTIME(local_datetime,tz_offset))=utc_date_only) \
  LEFT JOIN github_org ON (extra_github_org=github_org_name) \
  LEFT JOIN github_repo ON (extra_github_repo=github_repo_name) \
  WHERE local_datetime BETWEEN %s AND %s;"
  run_queries.run_dw_query(aggregate_query, (str(lower_limit),str(upper_limit)))

def import_dates():
  dw_mysql.import_dates_to_UTC('github',str(lower_limit),str(upper_limit))

def aggregate_to_contributor_facts():
  submit_patch_query="REPLACE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key,  \
  conversion_key, source_key,team_key) \
  SELECT canonical, utc_datetime, 1, utc_date_key,  \
  contributor_key, conversion_key, source_key, team_key \
  FROM github_facts  \
  INNER JOIN conversion ON (conversion_desc='Submitting patch') \
  INNER JOIN source ON (source_name='github') \
  INNER JOIN github_repo ON (github_facts.github_repo_key=github_repo.github_repo_key) \
  WHERE utc_datetime BETWEEN %s and %s \
  AND action='pull-request-opened' "
  run_queries.run_dw_query(submit_patch_query, (str(lower_limit),str(upper_limit)))

  merge_patch_query="REPLACE INTO contributor_facts \
  (canonical, utc_datetime, cnt, utc_date_key, contributor_key,  \
  conversion_key, source_key,team_key) \
  SELECT canonical, utc_datetime, 1, utc_date_key,  \
  contributor_key, conversion_key, source_key, team_key \
  FROM github_facts  \
  INNER JOIN conversion ON (conversion_desc='Having patch be merged') \
  INNER JOIN source ON (source_name='github') \
  INNER JOIN github_repo ON (github_facts.github_repo_key=github_repo.github_repo_key) \
  WHERE utc_datetime BETWEEN %s and %s \
  AND action='commit-author' "
  run_queries.run_dw_query(merge_patch_query, (str(lower_limit),str(upper_limit)))

# importing needs to wait until we have a netflow open
# and we modify the file to have ssl
import_github_activity()
populate_github_org()
populate_github_repo()
populate_contributor()
import_dates()
aggregate_to_github_facts()
aggregate_to_contributor_facts()

