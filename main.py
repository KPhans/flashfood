import os
from io import BytesIO
import gzip
import json

import pandas as pd
import boto3
from botocore.client import Config
import sqlite3
from sqlite3 import Error
from sqlalchemy import create_engine
from sqlalchemy.pool import SingletonThreadPool

access_key = "AKIASL5PJBWMKNUMYTBQ"
secret_key = "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"
bucket = "flashfood-engineering-assessment-data"

Region_host = os.environ.get("REGION_HOST")
Converted_Path = os.environ.get("Converted_path")
desktop_path = os.environ.get("desktop_path")

client = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(signature_version="s3v4"),
)

# We are using sqllite because db generation is really easy and we don't need any connection like sql server.

db_uri = "sqlite:///flashfood.db?check_same_thread=False"
engine = create_engine(db_uri)
connectionObject = sqlite3.connect("flashfood.db")
# Obtain a cursor object
cursorObject = connectionObject.cursor()


def create_table(table_name):
    createTable = (
        "CREATE TABLE '%s'(upc varchar(32), name varchar(32), category varchar(32), store_number varchar(32),"
        " price varchar(32), "
        "description varchar(32), taxable varchar(32),  department varchar(32), image varchar(32))"
        % table_name
    )  # Tp avoid sql injection
    cursorObject.execute(createTable)


def insert_row(
    table_name,
    upc,
    name,
    category,
    store_number,
    price,
    description,
    taxable,
    department,
    image,
):
    engine.execute(
        'INSERT INTO "%s" (upc, name, category, store_number, price, description, taxable, department, image) VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s" )'
        % (
            table_name,
            upc,
            name,
            category,
            store_number,
            price,
            description,
            taxable,
            department,
            image,
        )
    )


def query_function(bucket, key, upc, store_num):

    # remove trailing zeros
    upc = upc.lstrip("0")

    # Read in the data we want using the key (path to file we want)
    obj = client.get_object(Bucket=bucket, Key=key)
    with gzip.GzipFile(fileobj=obj["Body"]) as gzipfile:
        content = gzipfile.read()

    # Decode from bytes and transform to pandas df
    content = content.decode("utf-8")
    content = json.loads(content)
    content = pd.DataFrame(content)

    # remove duplicates and query for row with upc and store number required.
    df = content.drop_duplicates(subset=["upc", "store_number"], keep="first")
    df = df[(df["upc"] == upc) & (df["store_number"] == store_num)]

    # Take the subset and add it into our sqllite database (in this case a local flat db file.
    for index, row in df.iterrows():
        insert_row(
            row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]
        )
    return pd.read_sql("SELECT * FROM flash", engine)


create_table("flash")
query_function(
    bucket=bucket,
    key="ff-2020-03-dat-eng/ff__Cg9UaF0FdotHLuDKBmYPSkfMsGNy8nmh--store_mapping_docs__2020-03-09 16:35:26.008370.json.gz",
    upc="0061362434930",
    store_num="1006",
)

# What we attempt to do here is read in the data, from S3 specific on key. After this, we decode and convert to pd df
# where we can query this and push it into our database. Realistically, I would use something like redshift
# or snowflake as they are good with big data and fast with computes. Addition of a kafka stream would be
# desirable too. Not enough time obviously to incorporate these. Have not tested the code due to time but hopefully,
# The logic is sound.
