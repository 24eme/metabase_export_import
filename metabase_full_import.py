import sys

import metabase

metabase_apiurl = sys.argv[1]
metabase_username = sys.argv[2]
metabase_password = sys.argv[3]
metabase_basename = sys.argv[4]
import_dir = sys.argv[5]
sqlite_database_path_to_create = sys.argv[6]
user_to_create = sys.argv[7]
pass_to_create = sys.argv[8]

ametabase = metabase.MetabaseApi(metabase_apiurl, metabase_username, metabase_password)
# ametabase.debug = True

# ametabase.delete_database('base')
#
ametabase.create_database(metabase_basename, 'sqlite', {'db': sqlite_database_path_to_create})

user = user_to_create
ametabase.create_user(user, metabase_basename, {'first_name': 'User', 'last_name': metabase_basename})
ametabase.user_password(user, pass_to_create)
ametabase.membership_add(user, metabase_basename)
ametabase.permission_set_database(metabase_basename, metabase_basename, True, True)

print("fields (%s)\n" % metabase_basename)
ametabase.import_fields_from_csv(metabase_basename, import_dir)

print("collection and rights (%s)\n" % metabase_basename)
ametabase.create_collection(metabase_basename)
ametabase.create_collection('questions '+metabase_basename, metabase_basename)
ametabase.permission_set_collection(metabase_basename, metabase_basename, 'write')
ametabase.permission_set_collection(metabase_basename, 'questions '+metabase_basename, 'write')

print("metrics (%s)\n" % metabase_basename)
ametabase.import_metrics_from_json(metabase_basename, import_dir)
print("snippets (%s)\n" % metabase_basename)
ametabase.import_snippets_from_json(metabase_basename, import_dir, metabase_basename)
print("cards (%s)\n" % metabase_basename)
ametabase.import_cards_from_json(metabase_basename, import_dir, 'questions '+metabase_basename)
print("dashboards (%s)\n" % metabase_basename)
ametabase.import_dashboards_from_json(metabase_basename, import_dir, metabase_basename)
