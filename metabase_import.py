import metabase
import sys

metabase_apiurl = sys.argv[1]
metabase_username = sys.argv[2]
metabase_password = sys.argv[3]
metabase_base = sys.argv[4]
metabase_exportdir = sys.argv[5]

ametabase = metabase.MetabaseApi(metabase_apiurl, metabase_username, metabase_password)
#ametabase.debug = True

ametabase.import_fields_from_csv(metabase_base, metabase_exportdir)
ametabase.sync_scan_database(metabase_base)
ametabase.import_metrics_from_json(metabase_base, metabase_exportdir)
ametabase.import_cards_from_json(metabase_base, metabase_exportdir)
ametabase.import_dashboards_from_json(metabase_base, metabase_exportdir)
