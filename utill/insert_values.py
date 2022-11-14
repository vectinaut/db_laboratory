def insert_values(connection, table_name, columns, row):
    with connection.cursor() as cursor:
        q = "\'"
        query = f"""INSERT INTO public.{table_name} {str(tuple(columns)).replace(q, '')} VALUES {row};"""
        cursor.execute(
            str(query)
        )
