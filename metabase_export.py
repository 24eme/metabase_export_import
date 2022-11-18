import metabase
import sys
import os

metabase_apiurl = sys.argv[1]
metabase_username = sys.argv[2]
metabase_password = sys.argv[3]
metabase_base = sys.argv[4]
metabase_exportdir = sys.argv[5]

ametabase = metabase.MetabaseApi(metabase_apiurl, metabase_username, metabase_password)
#ametabase.debug = True

try:
    os.mkdir("export")
except:
    None

ametabase.export_fields_to_csv(metabase_base, metabase_exportdir)
ametabase.export_cards_to_json(metabase_base, metabase_exportdir)
ametabase.export_dashboards_to_json(metabase_base, metabase_exportdir)
ametabase.export_metrics_to_json(metabase_base, metabase_exportdir)
