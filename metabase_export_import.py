import sys
import pymysql.cursors
import csv

host = sys.argv[1]
user = sys.argv[2]
password = sys.argv[3]
db = sys.argv[4]

row_id_database_name = 0
row_id_table_name = 1
row_id_field_name = 2
row_id_semantic_type = 3
row_id_foreign_table = 4
row_id_foreign_field = 5
row_id_visibility_type = 6
row_id_has_field_values = 7
row_id_custom_position = 8
row_id_effective_type = 9
row_id_base_type = 10
row_id_database_type = 11

connection = pymysql.connect(host=host,
                             user=user,
                             password=password,
                             db=db
                            )



try:
    with connection.cursor() as cursor_read:
        # Create a new record
        sql = """SELECT metabase_database.name, metabase_database.id, metabase_table.name, metabase_table.id, metabase_field.name, metabase_field.id,
                 metabase_field.semantic_type, foreigntable.name, foreignfield.name,
                 metabase_field.visibility_type, metabase_field.has_field_values, metabase_field.custom_position,
                 metabase_field.effective_type, metabase_field.base_type, metabase_field.database_type
                 
                 FROM metabase_database, metabase_table, metabase_field
                 LEFT JOIN metabase_field as foreignfield ON metabase_field.fk_target_field_id = foreignfield.id
                 LEFT JOIN metabase_table as foreigntable ON foreignfield.table_id = foreigntable.id

                 WHERE metabase_field.table_id = metabase_table.id AND metabase_database.id = metabase_table.db_id ;"""
                 
        cursor_read.execute(sql)
        result = cursor_read.fetchall()
#        print(result)
        
        database_ids = dict()
        table_ids = dict()
        field_ids = dict()
        fields = dict()
    
        with open('metabase.csv', 'w', newline='', encoding='iso8859-1') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=';', quoting=csv.QUOTE_MINIMAL)
            csvwriter.writerow(["database name","table name","field name","semantic type","foreign table","foreign field","visibility type","has field values","custom_position","effective type","base type","database type"])
            for r in result:
                csvrow = [r[0], r[2], r[4], r[6],r[7],r[8],r[9],r[10],r[11],r[12],r[13],r[14]]
                csvwriter.writerow(csvrow)
                database_ids[csvrow[row_id_database_name]] = int(r[1])
                table_ids[csvrow[row_id_database_name]+'-'+csvrow[row_id_table_name]] = int(r[3])
                field_ids[csvrow[row_id_database_name]+'-'+csvrow[row_id_table_name]+'-'+csvrow[row_id_field_name]] = int(r[5])
                fields[csvrow[row_id_database_name]+'-'+csvrow[row_id_table_name]+'-'+csvrow[row_id_field_name]] = csvrow
                
        with open('metabase_new.csv', newline='', encoding='iso8859-1') as newstates:
            reader = csv.reader(newstates, delimiter=';', quoting=csv.QUOTE_MINIMAL, )
            for r in reader:
                try:
                    f = fields[r[row_id_database_name]+'-'+r[row_id_table_name]+'-'+r[row_id_field_name]]
                    if (not f):
                        continue
                    if f[row_id_visibility_type] == None:
                        f[row_id_visibility_type] = ''
                    if (r[row_id_visibility_type] != f[row_id_visibility_type]):
                        sql = "UPDATE metabase_field SET visibility_type = %s WHERE table_id = %s AND name = %s"
                        print(sql)
                        with connection.cursor() as cursor_update:
                            cursor_update.execute(sql, (
                                    r[row_id_visibility_type],
                                    int(table_ids[r[row_id_database_name]+'-'+r[row_id_table_name]]),
                                    r[2]
                                ))
                            connection.commit()
                            print("%s:%s/%s visibility_type changed (%s -> %s)\n" % (r[row_id_database_name], r[row_id_table_name], r[row_id_field_name], f[row_id_visibility_type], r[row_id_visibility_type]));

                    if f[row_id_custom_position] == None:
                        f[row_id_custom_position] = ''
                    if (r[row_id_custom_position] and r[row_id_custom_position] != f[row_id_custom_position]):
                        sql = "UPDATE metabase_field SET custom_position = %s WHERE table_id = %s AND name = %s"
                        print(sql)
                        with connection.cursor() as cursor_update:
                            cursor_update.execute(sql, (
                                    r[row_id_custom_position],
                                    int(table_ids[r[row_id_database_name]+'-'+r[row_id_table_name]]),
                                    r[row_id_field_name]
                                ))
                            connection.commit()
                            print("%s:%s/%s custom_position changed (%s -> %s)\n" % (r[row_id_database_name], r[row_id_table_name], r[row_id_field_name], f[row_id_custom_position], r[row_id_custom_position]));

                    if f[row_id_semantic_type] == None:
                        f[row_id_semantic_type] = ''
                    if f[row_id_foreign_table] == None:
                        f[row_id_foreign_table] = ''
                    if f[row_id_foreign_field] == None:
                        f[row_id_foreign_field] = ''
                    if r[row_id_semantic_type] == 'type/FK' and ( (f[row_id_semantic_type] != r[row_id_semantic_type]) or (f[row_id_foreign_table] != r[row_id_foreign_table]) or (f[row_id_foreign_field] != r[row_id_foreign_field]) ):
                        sql = "UPDATE metabase_field SET semantic_type = %s , fk_target_field_id = %s WHERE table_id = %s AND name = %s"
                        with connection.cursor() as cursor_update:
                            cursor_update.execute(sql, (
                                    r[row_id_semantic_type],
                                    int(field_ids[r[row_id_database_name]+'-'+r[row_id_foreign_table]+'-'+r[row_id_foreign_field]]),
                                    int(table_ids[r[row_id_database_name]+'-'+r[row_id_table_name]]),
                                    r[row_id_field_name]
                                ))
                            connection.commit()
                            print("%s:%s/%s semantic_type changed (%s:%s/%s -> %s:%s/%s)\n" % (r[row_id_database_name], r[row_id_table_name], r[row_id_field_name], f[row_id_semantic_type], f[row_id_foreign_table], f[row_id_foreign_field], r[row_id_semantic_type], r[row_id_foreign_table], r[row_id_foreign_field]));
                            continue;
                    if f[row_id_semantic_type] != r[row_id_semantic_type]:
                        sql = "UPDATE metabase_field SET semantic_type = %s WHERE table_id = %s AND name = %s"
                        with connection.cursor() as cursor_update:
                            cursor_update.execute(sql, (
                                    r[row_id_semantic_type],
                                    int(table_ids[r[row_id_database_name]+'-'+r[row_id_table_name]]),
                                    r[row_id_field_name]
                                ))
                            connection.commit()
                            print("%s:%s/%s semantic_type changed (%s -> %s)\n" % (r[row_id_database_name], r[row_id_table_name], r[row_id_field_name], f[row_id_semantic_type], r[row_id_semantic_type]));
                    
                except KeyError:
                    continue
                
finally:
    connection.close()
