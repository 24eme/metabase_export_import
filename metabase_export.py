import metabase
import sys

metabase_apiurl = sys.argv[1]
metabase_username = sys.argv[2]
metabase_password = sys.argv[3]
metabase_base = sys.argv[4]

ametabase = metabase.MetabaseApi(metabase_apiurl, metabase_username, metabase_password)
#ametabase.debug = True

#ametabase.delete_database('base')
#ametabase.create_database('base', 'sqlite', '/path/to/db.sqlite')

ametabase.export_fields_to_csv(metabase_base, metabase_base+'_fields_exported.csv')
ametabase.export_cards_to_json(metabase_base, metabase_base+'_cards_exported.json')
ametabase.export_dashboards_to_json(metabase_base, metabase_base+'_dashboard_exported.json')
