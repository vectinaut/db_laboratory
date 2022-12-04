import yaml
import logging
import psycopg2
import tqdm
import time

import random

from utill import *

access_config = yaml.safe_load(open('config/access.yaml'))

HOST = access_config['database']['host']
USER = access_config['database']['user']
PASSWORD = access_config['database']['password']
DB_NAME = access_config['database']['db_name']
PORT = access_config['database']['port']

gender_list = [0, 1]

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

try:
    connection = psycopg2.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DB_NAME
    )
    connection.autocommit = True
    # table_names = get_names.get_names_tabels(connection)
    table_names = {'user': 400,
                   'payments': 500,
                   'orders': 500,
                   'profession': 55,
                   'course': 120,
                   'prof_to_course': 300,
                   'order_details': 500,
                   'review': 400,
                   'module': 700,
                   'curator': 120,
                   'curator_to_module': 200,
                   'speaker': 70,
                   'speaker_to_module': 700,
                   'video': 2000,
                   'test': 300,
                   'practice': 500,
                   'progress': 1000}
    # print(table_names)

    all_ids_columns = []

    for table_name in table_names.keys():
        with connection.cursor() as cursor:
            query = f"""TRUNCATE public.{table_name} CASCADE;"""
            cursor.execute(
                query
            )
        columns = get_names.get_columns_names(connection, table_name)
        ids_columns = [name for name in columns if ('_id' in name and name not in all_ids_columns)]

        all_ids_columns.extend(ids_columns)
    # print(all_ids_columns)
    ids_dict = {id_name: (i + 1) * 10000 for i, id_name in enumerate(all_ids_columns)}

    pk_names_per_table = {get_pk_names(connection, table_name): table_name for table_name in table_names.keys()}

    for table_count, table_name in enumerate(table_names.keys()):
        logging.info(f'{table_name}: -----------{table_count + 1}/{len(table_names)}-----------')
        columns = get_names.get_columns_names(connection, table_name)
        pk_name = get_pk_names(connection, table_name)
        fk_names = get_fk_names(connection, table_name)
        # print(f"PK name: {pk_name}")
        for pk in range(1, table_names[table_name]+1):

            row = []
            gender = random.choice([0, 1])
            for column_count, column in enumerate(columns):
                # print(f'[INFO] {column}: {column_count + 1}/{len(columns)}')
                # check pk and fk
                if column in all_ids_columns:
                    if column == pk_name:
                        row.append(ids_dict[column] + pk)
                    else:
                        row.append(ids_dict[column] + random.randint(1, table_names[pk_names_per_table[column]]))

                elif 'name' in column:
                    name = get_name(column, gender)
                    row.append(name)

                elif column == 'gender':
                    row.append(str(gender))

                elif 'date' in column:
                    date = get_date(type_date=column)
                    row.append(date)

                elif column == 'phone_number':
                    phone = get_telephone()
                    row.append(phone)

                elif column == 'password':
                    password = get_password()
                    row.append(password)

                elif column == 'city':
                    city = get_city()
                    row.append(city)

                elif column == 'email':
                    email = get_email()
                    row.append(email)

                elif column == 'amount':
                    row.append(random.randint(1, 3))

                elif column == 'status':
                    row.append(random.choice(['successful', 'canceled']))

                elif column in ['description', 'title', 'comment']:
                    text = get_text(column)
                    row.append(text)

                elif column == 'price':
                    row.append(random.randint(50000, 210000))

                elif 'url' in column:
                    url = get_url()
                    row.append(url)

                elif column == 'rating':
                    row.append(random.randint(1, 10))

                elif column == 'education':
                    edc = get_education()
                    row.append(edc)

            row_tuple = tuple(row)
            # q = "\'"
            # print(str(tuple(columns)).replace(q, ''))
            # print(row_tuple)

            if len(row) < len(columns):
                logging.exception('The length of row less then list of columns')
                raise Exception

            insert_values(connection=connection,
                          table_name=table_name,
                          columns=columns,
                          row=row_tuple)


except Exception as _ex:
    logging.exception(f'Error while working with PostgreSQL {_ex}')
finally:
    if connection:
        connection.close()
        logging.info(f'PostgreSQL connection closer')
