import sys
import requests
import json
import csv

metabase_apiurl = sys.argv[1]
metabase_username = sys.argv[2]
metabase_password = sys.argv[3]
metabase_base = sys.argv[4]
#metabase_apiurl = "http://localhost/api/"

class Metabase:
    def __init__(self, apiurl, username, password, debug=False):
        self.apiurl = apiurl
        self.username = username
        self.password = password
        self.debug = debug
        
        self.metabase_session = None
        self.database_export = None
        
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
        elif method == 'PUT':
            r = requests.put(
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

    def field_id2tablenameandfieldname(self, database_name, field_id):
        if self.database_export is None:
            self.database_export = self.get_database(database_name, True)
        if not field_id:
            return ['', '']
        for table in self.database_export['tables']:
            for field in table['fields']:
                if field['id'] == field_id:
                    return [table['name'], field['name']]
        return ['', '']

    def field_tablenameandfieldname2field(self, database_name, table_name, field_name):
        if self.database_export is None:
            self.database_export = self.get_database(database_name, True)
        if not table_name or not field_name:
            return None
        for table in self.database_export['tables']:
            if table['name'] == table_name:
                for field in table['fields']:
                    if field['name'] == field_name:
                        return field
        return None
                    
                    

    def export_fields(self, database_name):
        self.database_export = self.get_database(database_name, True)
        result = []
        for table in self.database_export['tables']:
            table_name = table['name']
            for field in table['fields']:
                field_id = field['fk_target_field_id']
                [fk_table, fk_field] = self.field_id2tablenameandfieldname(database_name, field_id)
                result.append({
                                'table_name': table_name, 'field_name': field['name'], 'description': field['description'],
                                'semantic_type': str(field['semantic_type']),
                                'foreign_table': fk_table, 'foreign_field': fk_field,
                                'visibility_type': field['visibility_type'], 'has_field_values': field['has_field_values'],
                                'custom_position': str(field['custom_position']), 'effective_type': field['effective_type'],
                                'base_type': field['base_type'], 'database_type': field['database_type']
                              })
        return result
        
    def export_fields_to_csv(self, database_name, filename):
        export = metabase.export_fields(database_name)
        with open(filename, 'w', newline = '') as csvfile:
            my_writer = csv.writer(csvfile, delimiter = ',')
            need_header = True
            for row in export:
                if need_header:
                    my_writer.writerow(row.keys())
                    need_header = False
                my_writer.writerow(row.values())

    def import_fields_from_csv(self, database_name, filename):
        fields = []
        with open(filename, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                fields.append(row)
        return self.update_fields(database_name, fields)

    def update_fields(self, database_name, fields):
        output = []
        for f in fields:
            output.append(output.append(self.update_field(database_name, f)))
        return output

    def update_field(self, database_name, field):
        field_from_api = self.field_tablenameandfieldname2field(database_name, field['table_name'], field['field_name'])
        fk = self.field_tablenameandfieldname2field(database_name, field['foreign_table'], field['foreign_field'])
        field.pop('foreign_table')
        field.pop('foreign_field')
        data = {'id': str(field_from_api['id'])}
        for k in field.keys():
            if field[k]:
                data[k] = field[k]
            else:
                data[k] = None
        if fk :
            data['fk_target_field_id'] = fk['id']
        return self.query('PUT', 'field/'+data['id'], data)
            

metabase = Metabase(metabase_apiurl, metabase_username, metabase_password)
#metabase.debug = True
#metabase.create_database('base', 'sqlite', '/path/to/db.sqlite')
#metabase.delete_database('base')

#print(json.dumps(metabase.get_field('base', 'table', 'Field name')))

metabase.import_fields_from_csv(metabase_base, 'metabase_api_tobeimported.csv')
metabase.export_fields_to_csv(metabase_base, 'metabase_api_exported.csv')
