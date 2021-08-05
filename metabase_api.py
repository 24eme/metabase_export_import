import sys
import requests
import json
import csv

metabase_apiurl = sys.argv[1]
metabase_username = sys.argv[2]
metabase_password = sys.argv[3]
#metabase_apiurl = "http://localhost/api/"

class Metabase:
    def __init__(self, apiurl, username, password):
        self.apiurl = apiurl
        self.username = username
        self.password = password
        self.metabase_session = None
        self.debug = False

    def query (self, method, query_name, json_data = None):
        json_str = None
        if json_data is not None:
            json_str = json.dumps(json_data)
        
        headers =  { "Content-Type": "application/json;charset=utf-8" }
    
        if self.metabase_session is not None:
            headers["X-Metabase-Session"] = self.metabase_session
    
        query_url = metabase_apiurl+query_name
        
        if (self.debug):
            print(method+' '+query_url)
            print(headers)
            print(json_str)
        
        if method == 'POST':
            r = requests.post( 
                                query_url ,
                                data = json_str,
                                headers= headers
                             )
        elif method == 'GET':
            r = requests.get(
                                query_url,
                                data = json_str,
                                headers= headers
                            )        
        elif method == 'DELETE':
            r = requests.delete(
                                query_url,
                                data = json_str,
                                headers= headers
                                )        
        else:
            return {'errors': ['unkown method: '+method+' (GET,POST,DELETE allowed)']}
    
        if self.debug:
            print(r.text)
    
        try:
            query_response = r.json()
            if query_response.get('errors'):
                return query_response
        except AttributeError:
            return query_response
        except ValueError:
            if (r.text):
                return {'errors': [r.text]}
            return {}
            
        return query_response
    
    def create_session(self):
        json_response = self.query('POST', 'session', {"username": metabase_username, "password": metabase_password})
        try:
            self.metabase_session = json_response["id"]
        except KeyError:
            if json_response.get('errors'):
                print("ERROR: ",  end='')
                print(json_response['errors'])
            else:
                print("ERROR: enable to connect" + r.text)
            sys.exit(10)

    def create_session_if_needed(self):
        if self.metabase_session:
            return;
        self.create_session()
    
    def get_databases(self, full_info=False):
        self.create_session_if_needed()
        url = 'database'
        if (full_info):
            url += '?include=tables'
        databases = self.query('GET', url)
        return databases
        
    def create_database(self, name, engine, details, is_full_sync=True, is_on_demand=False, auto_run_queries=True):
        self.create_session_if_needed()
        data = self.get_database(name)
        if data:
            return data
        return self.query('POST', 'database', {"name": name, 'engine': engine, "details": details, "is_full_sync": is_full_sync, "is_on_demand": is_on_demand, "auto_run_queries": auto_run_queries})

    def get_database(self, name, full_info=False):
        name2database = {}
        for database in self.get_databases():
            name2database[database['name']] = database
        data = name2database.get(name)
        if not data:
            return {}
        if not full_info:
            return data
        return self.query('GET', 'database/'+str(data['id'])+'?include=tables.fields')

    def delete_database(self, name):
        self.create_session_if_needed()
        data = self.get_database(name)
        if not data:
            return
        return self.query('DELETE', 'database/'+str(data['id']), {'id': data['id']})

    def get_all_tables(self):
        self.create_session_if_needed()
        return self.query('GET', 'table')

    def get_tables_of_database(self, database_name):
        self.create_session_if_needed()
        result = self.get_database(database_name, True)
        try:
            return result['tables']
        except KeyError:
            return {}

    def get_table(self, database_name, table_name):
        for t in self.get_tables_of_database(database_name):
            if t['name'] == table_name:
                return t
        table = {}

    def get_field(self, database_name, table_name, field_name):
        table = self.get_table(database_name, table_name)
        try:
            for f in table['fields']:
                if f['name'] == field_name:
                    return f
        except TypeError:
            return {}
        return {}

    def delete_session(self):
        self.query('DELETE', 'session', {'metabase-session-id': self.metabase_session})
        self.metabase_session = None

    def fieldid2tableandfield(self, database_export, field_id):
        if not field_id:
            return ['', '']
        for table in database_export['tables']:
            for field in table['fields']:
                if field['id'] == field_id:
                    return [table['name'], field['name']]
        return ['', '']

    def export_fields(self, database_name):
        database_export = self.get_database(database_name, True)
        result = []
        for table in database_export['tables']:
            table_name = table['name']
            for field in table['fields']:
                field_id = field['fk_target_field_id']
                [fk_table, fk_field] = self.fieldid2tableandfield(database_export, field_id)
                result.append({
                                'database_name': database_name, 'table_name': table_name,
                                'field_name': field['name'], 'semantic_type': str(field['semantic_type']),
                                'foreign_table': fk_table, 'foreign_field': fk_field,
                                'visibility_type': field['visibility_type'], 'has_field_values': field['has_field_values'],
                                'custom_position': str(field['custom_position']), 'effective_type': field['effective_type'],
                                'base_type': field['base_type'], 'database_type': field['database_type']
                              })
        return result

metabase = Metabase(metabase_apiurl, metabase_username, metabase_password)
#metabase.debug = True
#metabase.create_database('base', 'sqlite', '/path/to/db.sqlite')
#metabase.delete_database('base')
#print(json.dumps(metabase.get_field('base', 'table', 'field')))

export = metabase.export_fields('igpvaucluse')
with open('metabase_api.csv', 'w', newline = '') as csvfile:
    my_writer = csv.writer(csvfile, delimiter = ';')
    need_header = True
    for row in export:
        if need_header:
            my_writer.writerow(row.keys())
            need_header = False
        my_writer.writerow(row.values())
