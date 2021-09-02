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

ametabase.import_fields_from_csv(metabase_base, metabase_base+'_fields__forimport.csv')
ametabase.import_metrics_from_json(metabase_base, metabase_base+'_metrics__forimport.csv')
ametabase.import_cards_from_json(metabase_base, metabase_base+'_cards_forimport.json')
ametabase.import_dashboards_from_json(metabase_base, metabase_base+'_dashboard_forimport.json')
