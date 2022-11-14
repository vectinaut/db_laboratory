import time
def get_names_tabels(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT table_name 
                FROM information_schema.tables
                WHERE table_schema = 'public'"""
        )
        names = cursor.fetchall()
        names = [name[0] for name in names]
        return names


def get_columns_names(connection, table_name):
    with connection.cursor() as cursor:
        cursor.execute(
            f"""SELECT COLUMN_NAME AS column_name
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_name='{table_name}'"""
        )
        names = cursor.fetchall()
        names = [name[0] for name in names]
        return names


def get_fk_names(connection, table_name):
    with connection.cursor() as cursor:
        cursor.execute(
            f"""SELECT C.COLUMN_NAME 
                FROM information_schema.TABLE_CONSTRAINTS AS pk


                INNER JOIN information_schema.KEY_COLUMN_USAGE AS C ON
                  C.TABLE_NAME = pk.TABLE_NAME AND
                  C.CONSTRAINT_NAME = pk.CONSTRAINT_NAME AND
                  C.TABLE_SCHEMA = pk.TABLE_SCHEMA


                WHERE  pk.TABLE_NAME  = '{table_name}' AND pk.TABLE_SCHEMA = 'public'
                  AND pk.CONSTRAINT_TYPE = 'FOREIGN KEY';"""
        )
        fk_names = cursor.fetchall()
        fk_names = [name[0] for name in fk_names]
        return fk_names


def get_pk_names(connection, table_name):
    with connection.cursor() as cursor:
        cursor.execute(
            f"""SELECT C.COLUMN_NAME 
                FROM information_schema.TABLE_CONSTRAINTS AS pk


                INNER JOIN information_schema.KEY_COLUMN_USAGE AS C ON
                  C.TABLE_NAME = pk.TABLE_NAME AND
                  C.CONSTRAINT_NAME = pk.CONSTRAINT_NAME AND
                  C.TABLE_SCHEMA = pk.TABLE_SCHEMA
                
                
                WHERE  pk.TABLE_NAME  = '{table_name}' AND pk.TABLE_SCHEMA = 'public'
                  AND pk.CONSTRAINT_TYPE = 'PRIMARY KEY';"""
        )
        pk_names = cursor.fetchall()
        pk_names = [name[0] for name in pk_names]

        fk_names = get_fk_names(connection, table_name)

        pk_names = list(set(pk_names) - set(fk_names))

        return pk_names[0]
