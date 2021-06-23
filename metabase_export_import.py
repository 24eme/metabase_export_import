import sys
import pymysql.cursors
import csv

host = sys.argv[1]
user = sys.argv[2]
password = sys.argv[3]
db = sys.argv[4]

connection = pymysql.connect(host=host,
                             user=user,
                             password=password,
                             db=db
                            )



try:
    with connection.cursor() as cursor_read:
        # Create a new record
        sql = "SELECT metabase_database.name, metabase_database.id, metabase_table.name, metabase_table.id, metabase_field.name, metabase_field.id, metabase_field.semantic_type, metabase_field.visibility_type, foreigntable.name, foreignfield.name, metabase_field.effective_type, metabase_field.has_field_values, metabase_field.base_type, metabase_field.database_type FROM metabase_database, metabase_table, metabase_field LEFT JOIN metabase_field as foreignfield ON metabase_field.fk_target_field_id = foreignfield.id LEFT JOIN metabase_table as foreigntable ON foreignfield.table_id = foreigntable.id WHERE metabase_field.table_id = metabase_table.id AND metabase_database.id = metabase_table.db_id ;"
        cursor_read.execute(sql)
        result = cursor_read.fetchall()
#        print(result)
        
        database_ids = dict()
        table_ids = dict()
        field_ids = dict()
        fields = dict()
    
        with open('metabase.csv', 'w', newline='', encoding='iso8859-1') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=';', quoting=csv.QUOTE_MINIMAL)
            csvwriter.writerow(["database","table name", "field name", "semantic type","visibility type","foreign table","foreign field","effective type", "has field value", "base type",  "database type"])
            for r in result:
                csvrow = [r[0], r[2], r[4], r[6],r[7],r[8],r[9],r[10],r[11],r[12],r[13]]
                csvwriter.writerow(csvrow)
                database_ids[csvrow[0]] = int(r[1])
                table_ids[csvrow[0]+'-'+csvrow[1]] = int(r[3])
                field_ids[csvrow[0]+'-'+csvrow[1]+'-'+csvrow[2]] = int(r[5])
                fields[csvrow[0]+'-'+csvrow[1]+'-'+csvrow[2]] = csvrow
                
        with open('metabase_new.csv', newline='', encoding='iso8859-1') as newstates:
            reader = csv.reader(newstates, delimiter=';', quoting=csv.QUOTE_MINIMAL, )
            for r in reader:
                try:
                    f = fields[r[0]+'-'+r[1]+'-'+r[2]]
                    if (f[3] == None):
                        f[3] = ''
                    if (f[4] == None):
                        f[4] = ''
                    if (f[5] == None):
                        f[5] = ''
                    if (f[6] == None):
                        f[6] = ''
                    if (r[3] != f[3]) or (r[4] != f[4]) or (r[5] != f[5]) or (r[6] != f[6]):
                        sql = "UPDATE metabase_field SET semantic_type = %s , visibility_type = %s , fk_target_field_id = %s WHERE table_id = %s AND name = %s"
                        print(sql % (r[3], r[4], field_ids[r[0]+'-'+r[5]+'-'+r[6]], table_ids[r[0]+'-'+r[1]], r[2]))
                        with connection.cursor() as cursor_update:
                            cursor_update.execute(sql, (r[3], r[4], int(field_ids[r[0]+'-'+r[5]+'-'+r[6]]), int(table_ids[r[0]+'-'+r[1]]), r[2]))
                            connection.commit()
                            print("%s:%s/%s changed (%s -> %s %s %s,%s -> %s)\n" % (r[0], r[1], r[2], f[3], r[3], r[5], r[6], f[4], r[4]));                        
                except KeyError:
                    continue
                
finally:
    connection.close()
