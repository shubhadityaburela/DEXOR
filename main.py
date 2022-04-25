# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import os
from minio import Minio
from minio.error import S3Error
import psycopg2
from psycopg2 import Error
import json, sys

from psycopg2._json import Json

folder_dir = "."


def upload():
    # Creating a client with MinIO server playground
    client = Minio(
        "minio.dexor.de",
        access_key="flea",
        secret_key="0be68c57a2568fba7a456f5b1de44611",
        region="eu",
    )

    # Make a "photos" bucket if does not exists
    # found = client.bucket_exists("photos")
    # if not found:
    #     client.make_bucket("photos")
    # else:
    #     print("Bucket 'photos' already exists")
    #     buckets = client.list_buckets()
    #     for bucket in buckets:
    #         print(bucket.name, bucket.creation_date)

    # Upload photos to the object storage

    for images in os.listdir(folder_dir):

        # check if the image ends with jpg
        if images.endswith(".jpg"):
            print(images)
            client.fput_object("photos", images, "./"+images)


if __name__ == '__main__':
    try:
        upload()
    except S3Error as exc:
        print("error occurred", exc)

    sys.exit()
    record_list = []
    # Loading JSON data into database
    for files in os.listdir(folder_dir):
        if files.endswith(".json"):
            with open(files) as data_file:
                record_list.append(json.load(data_file))

    if type(record_list) == list:
        columns = [list(x.keys()) for x in record_list][0]

    table_name = "json_data"
    sql_string = 'INSERT INTO {} '.format(table_name)
    sql_string += "(" + ', '.join(columns) + ")\nVALUES "

    # Enumerate over the record
    for i, record_dict in enumerate(record_list):
        # iterate over the values of each record dict object
        values = []
        for col_names, val in record_dict.items():

            # Postgres strings must be enclosed with single quotes
            if type(val) == str:
                # escape apostrophies with two single quotations
                val = val.replace("'", "''")
                val = "'" + val + "'"

            values += [str(val)]
        sql_string += "(" + ', '.join(values) + "),\n"

    # remove the last comma and end statement with a semicolon
    sql_string = sql_string[:-2] + ";"

    # value string for the SQL string
    values_str = ""

    # enumerate over the records' values
    for i, record in enumerate(values):

        # declare empty list for values
        val_list = []

        # append each value to a new list of values
        for v, val in enumerate(record):
            if type(val) == str:
                val = str(Json(val)).replace('"', '')
            val_list += [str(val)]

        # put parenthesis around each record string
        values_str += "(" + ', '.join(val_list) + "),\n"

    # remove the last comma and end SQL with a semicolon
    values_str = values_str[:-2] + ";"

    # concatenate the SQL string
    table_name = "json_data"
    sql_string = "INSERT INTO %s (%s)\nVALUES %s" % (
        table_name,
        ', '.join(columns),
        values_str
    )

    print("\nSQL statement:")
    print(sql_string)

    try:
        # Connect to PostgreSQL database
        connection = psycopg2.connect(
            host="db.dexor.de",
            user="flea",
            password="655ebdff6fecb8b6911b7a317a5e450b",
            port=5432,
            database="flea_photos"
        )
        # Create a cursor to perform database operations
        cursor = connection.cursor()
        # Print PostgreSQL details
        print("PostgreSQL server information")
        print(connection.get_dsn_parameters(), "\n")
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
        connection = None
        cursor = None

    if cursor is not None:
        try:
            cursor.execute(sql_string)
            connection.commit()

            print('\nfinished INSERT INTO execution')
        except(Exception, Error) as error:
            print("\nexecute_sql() error:", error)
            connection.rollback()

        cursor.close()
        connection.close()
