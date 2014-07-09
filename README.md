dw_contributor
==============

This repo contains scripts to populate the contributor data warehouse. There is one script, run_queries.py, which is not listed here as it contains passwords, but it just contains the code to run the queries directly to the database specified.

The topic scripts, like sumo.py and bugzilla.py, should be run first, 
with datetime limitations, for example:

sumo.py "2014-06-01 00:00:00" "2014-06-02 00:00:00"

After the topic scripts are run, the contributor_facts table is populated.

From there, run the aggregate_contributor_facts.py script in a similar manner
to populate the contributor_active table. 
