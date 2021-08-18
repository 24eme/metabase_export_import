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
        self.cards_export = None
        self.dashboards_name2id = dict()
        
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
        if isinstance(databases, list):
            return databases
        return databases['data']
        
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

    def table_id2name(self, database_name, table_id):
        if self.database_export is None:
            self.database_export = self.get_database(database_name, True)
        if not table_id:
            return ['', '']
        for table in self.database_export['tables']:
            if table['id'] == table_id:
                return table['name']
        return ''

    def card_id2name(self, database_name, card_id):
        if self.cards_export is None:
            self.cards_export = self.get_cards(database_name)
        for card in self.cards_export:
            if card['id'] == card_id:
                return card['name']
        return ''

    def field_tablenameandfieldname2field(self, database_name, table_name, field_name):
        if self.database_export is None:
            self.database_export = self.get_database(database_name, True)
        if not table_name or not field_name:
            return None
        if not self.database_export.get('tables'):
            return None
        for table in self.database_export['tables']:
            if table['name'] == table_name:
                for field in table['fields']:
                    if field['name'] == field_name:
                        return field
        return None
    
    def table_name2id(self, database_name, table_name):
        if self.database_export is None:
            self.database_export = self.get_database(database_name, True)
        if not table_name:
            return None
        if not self.database_export.get('tables'):
            return None
        for table in self.database_export['tables']:
            if table['name'] == table_name:
                return table['id']
        return None
                    
    def export_fields(self, database_name):
        self.database_export = self.get_database(database_name, True)
        result = []
        if not self.database_export.get('tables'):
            return None
        for table in self.database_export['tables']:
            table_name = table['name']
            for field in table['fields']:
                field_id = field['fk_target_field_id']
                [fk_table, fk_field] = self.field_id2tablenameandfieldname(database_name, field_id)
                if not field['semantic_type']:
                    field['semantic_type'] = ''
                if not field['custom_position']:
                    field['custom_position'] = ''
                result.append({
                                'table_name': table_name, 'field_name': field['name'], 'description': field['description'],
                                'semantic_type': field['semantic_type'],
                                'foreign_table': fk_table, 'foreign_field': fk_field,
                                'visibility_type': field['visibility_type'], 'has_field_values': field['has_field_values'],
                                'custom_position': field['custom_position'], 'effective_type': field['effective_type'],
                                'base_type': field['base_type'], 'database_type': field['database_type'], 'field_id': field['id']
                              })
        return result
        
    def export_fields_to_csv(self, database_name, filename):
        export = metabase.export_fields(database_name)
        if not export:
            return
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
        if not field_from_api:
            return None
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

    def database_name2id(self, database_name):
        if self.database_export is None:
            self.database_export = self.get_database(database_name, True)
        if not self.database_export.get('id'):
            return None
        return self.database_export['id']
            
    def get_cards(self, database_name):
        database_id = self.database_name2id(database_name)
        return self.query('GET', 'card?f=database&model_id='+str(database_id))

    def get_collections(self, database_name):
        database_id = self.database_name2id(database_name)
        return self.query('GET', 'collection')

    def get_dashboards(self, database_name):
        database_id = self.database_name2id(database_name)
        dashbords_light = self.query('GET', 'dashboard')
        dashboards = []
        for d in dashbords_light:
            res = self.query('GET', 'dashboard/'+str(d['id']))
            good_db = True
            for c in res['ordered_cards']:
                if c['card']['database_id'] != database_id:
                    good_db = False
                    continue
            if not good_db:
                continue
            dashboards.append(res)
        return dashboards

    def dashboard_name2id(self, database_name, dashboard_name):
        if not self.dashboards_name2id:
            for d in self.get_dashboards(database_name):
                self.dashboards_name2id[d['name']] = d['id']
        return self.dashboards_name2id.get(dashboard_name)

    def convert_pcnames2id(self, database_name, fieldname, pcnames):
        if pcnames[0] != '%':
            return [None, None]
        pcres = pcnames.split('%')
        if len(pcres) != 3:
            return [None, None]
        [empty, new_k, names] = pcres
        if new_k == 'JSONCONV':
            return 'TODO'
        if fieldname == 'database_name':
            return [new_k, self.database_name2id(database_name)]
        resplit = names.split('|')
        if len(resplit) == 2:
            field = self.field_tablenameandfieldname2field(database_name, resplit[0], resplit[1])
            if field:
                return[new_k, field['id']]
        if len(resplit) == 1:
            table_id = self.table_name2id(database_name, resplit[0])
            return [new_k, table_id]
        return [None, None]

    def convert_names2ids(self, database_name, obj):
        obj_res = obj
        if isinstance(obj, list):
            if len(obj) and obj[0] == 'field':
                if obj[1][0] == '%':
                    [k_name, value] = self.convert_pcnames2id(database_name, None, obj[1])
                    obj_res[1] = value
            else:
                for i in range(len(obj)):
                    obj_res[i] = self.convert_names2ids(database_name, obj[i])
        elif isinstance(obj, dict):
            obj_res = obj.copy()
            for k in obj.keys():
                if k[0] == '%':
                    [new_k, value] = self.convert_pcnames2id(database_name, None, k)
                    obj_res.pop(k)
                    obj_res[new_k] = obj[k]
                elif k in ['field_name', 'table_name', 'database_name'] and obj[k][0] == '%':
                    [new_k, value] = self.convert_pcnames2id(database_name, k, obj[k])
                    obj_res.pop(k)
                    obj_res[new_k] = value
        return obj_res

    def convert_ids2names(self, database_name, obj, previous_key):
        obj_res = obj
        if isinstance(obj, list):
            if len(obj) and obj[0] == 'field':
                try:
                    [t, f] = self.field_id2tablenameandfieldname(database_name, int(obj_res[1]))
                    obj_res[1] = '%%'+t+'|'+f
                except ValueError:
                    obj_res[1] = obj[1]
            else:
                for i in range(len(obj)):
                    obj_res[i] = self.convert_ids2names(database_name, obj[i], previous_key)
        elif isinstance(obj, dict):
            obj_res = obj.copy()
            for k in obj.keys():
                if isinstance(obj[k], dict) or isinstance(obj[k], list):
                    k_previous = previous_key
                    k2int = None
                    try:
                        k2int = int(k)
                        k_name = k
                        if k2int:
                            [t, f] = self.field_id2tablenameandfieldname(database_name, k2int)
                            k_name = '%%'+t+'|'+f
                    except ValueError:
                        k_name = k
                        k_previous = k
                    if not k2int:
                        try:
                            k_data = json.loads(k)
                            if k_data[0] == 'ref':
                                [t, f] = self.field_id2tablenameandfieldname(database_name, int(k_data[1][1]))
                                k_data[1][1] = '%%'+t+'|'+f
                                k_name = '%JSONCONV%'+json.dumps(k_data)
                            else:
                                k_name = k
                        except json.decoder.JSONDecodeError:
                            k_name = k
                            k_previous = k
                    obj_res.pop(k)
                    obj_res[k_name] = self.convert_ids2names(database_name, obj[k], k_previous)
                else:
                    if k in ['field_id'] or (k == 'id' and previous_key in ['result_metadata', 'param_fields']):
                        id = obj_res.pop(k)
                        if id:
                            [t, f] = self.field_id2tablenameandfieldname(database_name, int(id))
                            obj_res['field_name'] = '%'+k+'%'+t+'|'+f
                    elif k in ['table_id', 'source-table']:
                        id = obj_res.pop(k)
                        if id:
                            t = self.table_id2name(database_name, int(id))
                            obj_res['table_name'] = '%'+k+'%'+t
                    elif k in ['card_id']:
                        id = obj_res.pop(k)
                        if id:
                            n = self.card_id2name(database_name, int(id))
                            obj_res['card_name'] = '%'+k+'%'+n
        return obj_res

    def export_dashboards_to_json(self, database_name, filename):
        export = metabase.get_dashboards(database_name)
        with open(filename, 'w', newline = '') as jsonfile:
            jsonfile.write(json.dumps(self.convert_ids2names(database_name, export, None)))

metabase = Metabase(metabase_apiurl, metabase_username, metabase_password)
#metabase.debug = True
#metabase.create_database('base', 'sqlite', '/path/to/db.sqlite')
#metabase.delete_database('base')

#print(json.dumps(metabase.get_field('base', 'table', 'Field name')))

metabase.import_fields_from_csv(metabase_base, 'metabase_api_tobeimported.csv')
metabase.export_fields_to_csv(metabase_base, 'metabase_api_exported.csv')

metabase.export_dashboards_to_json(metabase_base, 'metabase_api_exported.json')

