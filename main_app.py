import time
import csv
import re

from Tracker.main import Tracker
import os
import pandas as pd
import json
from flask import Flask, render_template, request, Blueprint, session
from werkzeug.utils import secure_filename
import glob
import fileinput
import sys
import matplotlib.pyplot as plt
import io
import base64
import psycopg2
import uuid
import itertools
from itertools import product
from queries import (
    mysql_queries,
    mongodb_queries,
    postgresql_queries,
    couchbase_queries,
)
from datetime import datetime
from decimal import Decimal
from Tracker.main import run_format_operation
from Tracker.main import run_experiment
from Tracker.main import run_geometry_operation
from Tracker.main import run_index_operation
from Tracker.main import run_compression_operation

sys.path.insert(0, "./")

app = Flask(__name__)
app.secret_key = "secret"


@app.route("/")
def index():
    return render_template("dbJoules.html")


@app.route("/upload_file", methods=["GET", "POST"])
def upload_file():
    return render_template("upload.html")


@app.route("/column_types", methods=["POST"])
def upload():
    if request.method == "POST":
        f = request.files
        print(f"this {f}")
        f = request.files["file"]
        filename = f.filename
        session["filename"] = filename
        filename1 = filename.split(".")
        table = filename1[0].replace(" ", "_")
        table_name = table.lower()
        session["table_name"] = table_name
        if f.filename == "":
            not_uploaded = "Please select a file to upload."
            return render_template("upload.html", not_uploaded=not_uploaded)
        if f and allowed_file(f.filename):
            upload_folder = "uploads"
            empty_folder(upload_folder)
            fp = os.path.join("uploads", f.filename)
            f.save(fp)
            if os.path.isfile(fp):
                data = []
                with open(fp, "r") as csv_file:
                    csv_reader = csv.reader(csv_file)
                    data = list(csv_reader)
                    column_names = data[0]
                    column_names = [
                        replace_spaces_with_underscore(item) for item in column_names
                    ]
                    session["column_names"] = column_names
                    new_data = data[1:]
                    new_fp = os.path.join("uploads", "new_" + f.filename)
                    with open(new_fp, "w", newline="") as new_csv_file:
                        csv_writer = csv.writer(new_csv_file)
                        csv_writer.writerows(new_data)
                os.remove(fp)
                os.rename(new_fp, fp)
                log_file(f.filename, data)
                return render_template("column_names.html", items=column_names)
            else:
                return "File not found."


@app.route("/queries", methods=["POST", "GET"])
def table_creation():
    if request.method == "POST":
        mysql_username = request.form["mysql_username"]
        mysql_db_name = request.form["mysql_db_name"]
        mysql_password = request.form["mysql_password"]
        mongodb_db_name = request.form["mongodb_db_name"]
        postgresql_username = request.form["postgresql_username"]
        postgresql_db_name = request.form["postgresql_db_name"]
        postgresql_password = request.form["postgresql_password"]
        couchbase_username = request.form["couchbase_username"]
        couchbase_password = request.form["couchbase_password"]

        session["mysql_username"] = mysql_username
        session["mysql_db_name"] = mysql_db_name
        session["mysql_password"] = mysql_password
        session["mongodb_db_name"] = mongodb_db_name
        session["postgresql_username"] = postgresql_username
        session["postgresql_db_name"] = postgresql_db_name
        session["postgresql_password"] = postgresql_password
        session["couchbase_username"] = couchbase_username
        session["couchbase_password"] = couchbase_password

        table_name = session.get("table_name", None)

        a = create_mysql_table(mysql_username, mysql_password, mysql_db_name)
        b = create_mongodb_collection(mongodb_db_name)
        c = create_postgresql_table(
            postgresql_username, postgresql_password, postgresql_db_name
        )
        d = create_couchbase_collection(couchbase_username, couchbase_password)
        if a == "success" and b == "success" and c == "success" and d == "success":
            return render_template("csv_choice.html", table_name=table_name)
        else:
            return "Unsuccessfull!"
    elif request.method == "GET":
        return render_template("upload.html")


@app.route("/primary_key_choice", methods=["POST"])
def submit_columns():
    column_names = session.get("column_names", None)
    column_types = [request.form.get(item) for item in column_names]
    session["column_types"] = column_types
    return render_template("primary_key_choice.html")


@app.route("/is_prim_key", methods=["GET", "POST"])
def is_prim_key():
    column_names = session.get("column_names", None)
    return render_template("primary_key.html", items=column_names)


@app.route("/no_prim_key", methods=["GET", "POST"])
def no_prim_key():
    return render_template("database_choice.html")


@app.route("/database_choice_details", methods=["GET", "POST"])
def prim_key():
    column_names = session.get("column_names", None)
    primary_key = request.form.get("primary_key")
    primary_key = primary_key.replace(" ", "_")
    session["primary_key"] = primary_key
    return render_template("database_choice.html")


@app.route("/database_choice_create_table", methods=["GET", "POST"])
def database_choice_details():
    databases = request.form.getlist("databases")
    session["databases"] = databases
    return render_template("database_choice_details.html", databases=databases)


@app.route("/output_choice", methods=["POST"])
def database_choice_get_queries():
    databases = session.get("databases", None)
    database_info = {}

    for database in databases:
        info = {}
        if database != "MongoDB":
            info["user"] = request.form.get(f"{database}_user")
            info["password"] = request.form.get(f"{database}_password")
        info["name"] = request.form.get(f"{database}_name")
        database_info[database] = info

    session["database_info"] = database_info
    table_name = session.get("table_name", None)
    results = {}

    for database, info in database_info.items():
        if database == "MySQL":
            results["MySQL"] = create_mysql_table(
                info["user"], info["password"], info["name"]
            )
        elif database == "PostgreSQL":
            results["PostgreSQL"] = create_postgresql_table(
                info["user"], info["password"], info["name"]
            )
        elif database == "MongoDB":
            results["MongoDB"] = create_mongodb_collection(info["name"])
        elif database == "Couchbase":
            results["Couchbase"] = create_couchbase_collection(
                info["user"], info["password"]
            )

    r = any(results[db] == "success" for db in databases)
    if r:
        return render_template("csv_choice.html", table_name=table_name)
    else:
        return "Unsuccessfull!"


@app.route("/results", methods=["POST"])
def database_choice_queries():
    databases = session.get("databases", None)
    database_info = session.get("database_info", None)
    queries = []
    for database in databases:
        textarea = request.form.get(database)
        queries.append(textarea)
    session["queries"] = queries

    query_results = {}
    for i in range(0, len(databases)):
        if databases[i] == "MySQL":
            query_results["MySQL"] = execute_mysql_query(
                queries[i],
                database_info["MySQL"]["user"],
                database_info["MySQL"]["password"],
                database_info["MySQL"]["name"],
            )
            time.sleep(1)
        elif databases[i] == "PostgreSQL":
            query_results["PostgreSQL"] = execute_postgreSQL_query(
                queries[i],
                database_info["PostgreSQL"]["user"],
                database_info["PostgreSQL"]["password"],
                database_info["PostgreSQL"]["name"],
            )
            time.sleep(1)
        elif databases[i] == "MongoDB":
            query_results["MongoDB"] = execute_mongodb_query(
                queries[i], database_info["MongoDB"]["name"]
            )
            time.sleep(1)
        elif databases[i] == "Couchbase":
            query_results["Couchbase"] = execute_couchbase_query(
                queries[i],
                database_info["Couchbase"]["user"],
                database_info["Couchbase"]["password"],
                database_info["Couchbase"]["name"],
            )
            time.sleep(1)

    sorted_data = {
        k: v
        for k, v in sorted(query_results.items(), key=lambda item: float(item[1][2]))
    }
    return render_template("result.html", data=query_results, sorted_data=sorted_data)


@app.route("/choice")
def choice():
    return render_template("choice.html")


@app.route("/input-queries")
def queries():
    databases = session.get("databases", None)
    return render_template("database_choice_queries.html", databases=databases)


@app.route("/existing_database_choice")
def existing_database_choice():
    return render_template("existing_database_choice.html")


@app.route("/existing_database_details", methods=["POST"])
def existing_database_databases():
    databases = request.form.getlist("databases")
    session["databases"] = databases
    return render_template("existing_database_details.html", databases=databases)


@app.route("/existing_database", methods=["POST"])
def existing_database_details():
    databases = session.get("databases", None)
    database_info = {}

    for database in databases:
        info = {}
        if database != "MongoDB":
            info["user"] = request.form.get(f"{database}_user")
            info["password"] = request.form.get(f"{database}_password")
        info["name"] = request.form.get(f"{database}_name")
        info["query"] = request.form.get(f"{database}_query")
        database_info[database] = info

    session["database_info"] = database_info
    query_results = {}

    for i in range(0, len(databases)):
        if databases[i] == "MySQL":
            query_results["MySQL"] = execute_mysql_query(
                database_info["MySQL"]["query"],
                database_info["MySQL"]["user"],
                database_info["MySQL"]["password"],
                database_info["MySQL"]["name"],
            )
            time.sleep(1)
        elif databases[i] == "PostgreSQL":
            query_results["PostgreSQL"] = execute_postgreSQL_query(
                database_info["PostgreSQL"]["query"],
                database_info["PostgreSQL"]["user"],
                database_info["PostgreSQL"]["password"],
                database_info["PostgreSQL"]["name"],
            )
            time.sleep(1)
        elif databases[i] == "MongoDB":
            query_results["MongoDB"] = execute_mongodb_query(
                database_info["MongoDB"]["query"], database_info["MongoDB"]["name"]
            )
            time.sleep(1)
        elif databases[i] == "Couchbase":
            query_results["Couchbase"] = execute_couchbase_query(
                database_info["Couchbase"]["query"],
                database_info["Couchbase"]["user"],
                database_info["Couchbase"]["password"],
                database_info["Couchbase"]["name"],
            )
            time.sleep(1)

    sorted_data = {
        k: v
        for k, v in sorted(query_results.items(), key=lambda item: float(item[1][2]))
    }
    return render_template("result.html", data=query_results, sorted_data=sorted_data)


def allowed_file(filename):
    print(filename.rsplit(".", 1)[1].lower())
    return "." in filename and filename.rsplit(".", 1)[1].lower() == "csv"


def empty_folder(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                os.rmdir(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}: {e}")


def replace_spaces_with_underscore(item):
    return item.replace(" ", "_")


def log_file(file_name, data):
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    log_file_name = f"{timestamp}_{file_name}.json"
    log_file_path = os.path.join("logfiles", log_file_name)
    with open(log_file_path, "w") as f:
        json.dump(data, f, indent=4)


def generate_unique_key():
    return str(uuid.uuid4())


def create_mysql_table(user, password, database):
    conn = mysql.connector.connect(
        host="localhost", user=user, passwd=password, database=database
    )
    cur = conn.cursor()
    column_names = session.get("column_names", None)
    column_types = session.get("column_types", None)
    filename = session.get("filename", None)
    table_name = session.get("table_name", None)
    primary_key = session.get("primary_key", None)

    columns = [(x, y) for x, y in zip(column_names, column_types)]

    drop_query = f"DROP TABLE IF EXISTS {table_name};"
    create_query = f"CREATE TABLE {table_name} ("
    for column_name, column_type in columns:
        create_query += f"{column_name} {column_type}, "
    create_query = create_query[:-2]
    create_query += ");"

    path = "uploads/" + filename
    data = csv.reader(open(path))
    insert_query = f"INSERT INTO {table_name} ("
    for x in column_names:
        insert_query += f"{x}, "
    insert_query = insert_query[:-2]
    insert_query += ") VALUES ("
    placeholders = ", ".join(["%s"] * len(column_names))
    insert_query += placeholders
    insert_query += ")"

    cur.execute(drop_query)
    cur.execute(create_query)
    for row in data:
        cur.execute(insert_query, row)

    conn.commit()
    cur.close()
    conn.close()
    print("Table created and data inserted in MySQL.")
    return "success"


def create_postgresql_table(user, password, database):
    conn = psycopg2.connect(
        host="localhost", user=user, password=password, database=database
    )
    cur = conn.cursor()
    column_names = session.get("column_names", None)
    column_types = session.get("column_types", None)
    filename = session.get("filename", None)
    table_name = session.get("table_name", None)
    primary_key = session.get("primary_key", None)

    columns = [(x, y) for x, y in zip(column_names, column_types)]

    drop_query = f"DROP TABLE IF EXISTS {table_name};"
    create_query = f"CREATE TABLE {table_name} ("
    for column_name, column_type in columns:
        if column_name == primary_key:
            create_query += f"{column_name} {column_type} PRIMARY KEY, "
        else:
            create_query += f"{column_name} {column_type}, "
    create_query = create_query[:-2]
    create_query += ");"

    path = "uploads/" + filename
    data = csv.reader(open(path))
    insert_query = f"INSERT INTO {table_name} ("
    for x in column_names:
        insert_query += f"{x}, "
    insert_query = insert_query[:-2]
    insert_query += ") VALUES ("
    placeholders = ", ".join(["%s"] * len(column_names))
    insert_query += placeholders
    insert_query += ")"

    cur.execute(drop_query)
    cur.execute(create_query)
    for row in data:
        cur.execute(insert_query, row)

    conn.commit()
    cur.close()
    conn.close()
    print("Table created and data inserted in PostgreSQL.")
    return "success"


def create_mongodb_collection(db_name):
    client = MongoClient("mongodb://localhost:27017/")
    collection_name = session.get("table_name", None)
    column_names = session.get("column_names", None)
    column_types = session.get("column_types", None)
    filename = session.get("filename", None)

    db = client[db_name]
    if collection_name in db.list_collection_names():
        db.drop_collection(collection_name)
    collection = db[collection_name]

    path = "uploads/" + filename
    with open(path, "r") as file:
        reader = csv.DictReader(file, fieldnames=column_names)
        for row in reader:
            converted_row = {}
            for col_name, col_type in zip(column_names, column_types):
                value = row[col_name]
                if col_type == "int":
                    converted_row[col_name] = int(row[col_name])
                elif col_type == "float":
                    converted_row[col_name] = float(row[col_name])
                elif col_type == "bool":
                    converted_row[col_name] = row[col_name].lower() == "true"
                elif col_type == "date":
                    converted_row[col_name] = datetime.strptime(value, "%Y-%m-%d")
                else:
                    converted_row[col_name] = row[col_name]
            collection.insert_one(converted_row)

    print("Collection created and data inserted in MongoDB.")
    return "success"


def create_couchbase_collection(user, password):
    try:
        cluster = Cluster(
            "couchbase://localhost",
            ClusterOptions(PasswordAuthenticator(user, password)),
        )
        auth = PasswordAuthenticator(user, password)
        conn = Cluster.connect("couchbase://localhost", ClusterOptions(auth))

        column_names = session.get("column_names", None)
        column_types = session.get("column_types", None)
        filename = session.get("filename", None)
        bucket_name = session.get("table_name", None)
        primary_key = session.get("primary_key", None)

        existing_buckets = cluster.buckets().get_all_buckets()
        if bucket_name in [bucket.name for bucket in existing_buckets]:
            cluster.buckets().drop_bucket(bucket_name)

        settings = CreateBucketSettings(
            name=bucket_name, bucket_type="couchbase", ram_quota_mb=100, num_replicas=1
        )
        cluster.buckets().create_bucket(settings)
        time.sleep(2)

        bucket = cluster.bucket(bucket_name)
        collection = bucket.default_collection()

        path = "uploads/" + filename
        with open(path, "r") as csv_file:
            data = csv.reader(csv_file)
            for row in data:
                doc = {}
                for i, (name, type) in enumerate(zip(column_names, column_types)):
                    if type == "int":
                        value = int(row[i])
                    elif type == "float":
                        value = float(row[i])
                    elif type == "date":
                        value = datetime.strptime(row[i], "%Y-%m-%d").date()
                        value = value.isoformat()
                    else:
                        value = row[i]
                    doc[name] = value
                unique_key = generate_unique_key()
                collection.upsert(unique_key, doc)

        query = f"CREATE PRIMARY INDEX ON {bucket_name} USING GSI;"
        res = execute_couchbase_query(query, user, password, bucket_name)
        print("Bucket created and data inserted in Couchbase.")
        return "success"
    except Exception as e:
        print(f"Error: {str(e)}")


@app.route("/generate_csv")
def generate_csv():
    database_info = session.get("database_info")
    file_name = "DBJoules_output.csv"

    fieldNames = []
    for key, value in database_info.items():
        fieldNames.append(key + " Query")
        fieldNames.append("CPU consumption")
        fieldNames.append("RAM consumption")
        fieldNames.append("Total consumption")
        fieldNames.append("Time Taken")

    data = {}
    for key, value in database_info.items():
        if key == "MySQL":
            data["MySQL"] = mysql_queries
        elif key == "PostgreSQL":
            data["PostgreSQL"] = postgresql_queries
        elif key == "MongoDB":
            data["MongoDB"] = mongodb_queries
        elif key == "Couchbase":
            data["Couchbase"] = couchbase_queries

    action_functions = {
        "MySQL": execute_mysql_query,
        "PostgreSQL": execute_postgreSQL_query,
        "MongoDB": execute_mongodb_query,
        "Couchbase": execute_couchbase_query,
    }
    database_keys = list(data.keys())
    outputs = {key: [] for key in database_keys}

    for key in database_keys:
        if key == "MySQL":
            for i in range(len(mysql_queries)):
                query = mysql_queries[i]
                for j in range(10):
                    l = [query]
                    x = execute_mysql_query(
                        query,
                        database_info[key]["user"],
                        database_info[key]["password"],
                        database_info[key]["name"],
                    )
                    l.extend(x)
                    outputs[key].append(l)
                    time.sleep(1)
        elif key == "PostgreSQL":
            for i in range(len(postgresql_queries)):
                query = postgresql_queries[i]
                for j in range(10):
                    l = [query]
                    x = execute_postgreSQL_query(
                        query,
                        database_info[key]["user"],
                        database_info[key]["password"],
                        database_info[key]["name"],
                    )
                    l.extend(x)
                    outputs[key].append(l)
                    time.sleep(1)
        elif key == "MongoDB":
            for i in range(len(mongodb_queries)):
                query = mongodb_queries[i]
                for j in range(10):
                    l = [query]
                    x = execute_mongodb_query(query, database_info[key]["name"])
                    l.extend(x)
                    outputs[key].append(l)
                    time.sleep(1)
        elif key == "Couchbase":
            for i in range(len(couchbase_queries)):
                query = couchbase_queries[i]
                for j in range(10):
                    l = [query]
                    x = execute_couchbase_query(
                        query,
                        database_info[key]["user"],
                        database_info[key]["password"],
                        database_info[key]["name"],
                    )
                    l.extend(x)
                    outputs[key].append(l)
                    time.sleep(1)

    csv_data = [fieldNames]
    headers = list(outputs.keys())
    for i in range(0, len(outputs[headers[0]])):
        data_row = []
        for db in headers:
            for x in range(len(outputs[db][i])):
                data_row.append(outputs[db][i][x])
        csv_data.append(data_row)

    path = "results/" + file_name
    with open(path, "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(csv_data)

    print("csv generated")
    return render_template("csv_output.html")


@app.route("/enter_queries", methods=["POST", "GET"])
def enter_queries():
    if request.method == "POST":
        mysql_query = request.form["mysql_query"]
        mongodb_query = request.form["mongodb_query"]
        postgresql_query = request.form["postgresql_query"]
        couchbase_query = request.form["couchbase_query"]

        mysql_username = session.get("mysql_username", None)
        mysql_db_name = session.get("mysql_db_name", None)
        mysql_password = session.get("mysql_password", None)
        mongodb_db_name = session.get("mongodb_db_name", None)
        postgresql_username = session.get("postgresql_username", None)
        postgresql_db_name = session.get("postgresql_db_name", None)
        postgresql_password = session.get("postgresql_password", None)
        couchbase_username = session.get("couchbase_username", None)
        couchbase_password = session.get("couchbase_password", None)
        couchbase_bucket_name = session.get("table_name", None)

        time.sleep(1)
        mysql_res = execute_mysql_query(
            mysql_query, mysql_username, mysql_password, mysql_db_name
        )
        time.sleep(1)
        postgresql_res = execute_postgreSQL_query(
            postgresql_query,
            postgresql_username,
            postgresql_password,
            postgresql_db_name,
        )
        time.sleep(1)
        mongodb_res = execute_mongodb_query(mongodb_query, mongodb_db_name)
        time.sleep(1)
        couchbase_res = execute_couchbase_query(
            couchbase_query,
            couchbase_username,
            couchbase_password,
            couchbase_bucket_name,
        )
        time.sleep(1)

        databases = ["MySQl", "PostgreSQl", "Mongodb", "Couchbase"]
        eff_total_consumption = [
            mysql_res[2],
            postgresql_res[2],
            mongodb_res[2],
            couchbase_res[2],
        ]
        databases_total_dict = {
            index: name for index, name in zip(eff_total_consumption, databases)
        }
        sorted_total = [
            databases_total_dict[i]
            for i in sorted(databases_total_dict, key=lambda x: Decimal(x))
        ]

        return render_template(
            "compare_result.html",
            mysql_cpu_consumption_j=mysql_res[0],
            mysql_ram_consumption_j=mysql_res[1],
            mysql_total_consumption_j=mysql_res[2],
            mysql_time_taken=mysql_res[3],
            postgresql_cpu_consumption_j=postgresql_res[0],
            postgresql_ram_consumption_j=postgresql_res[1],
            postgresql_total_consumption_j=postgresql_res[2],
            postgresql_time_taken=postgresql_res[3],
            mongodb_cpu_consumption_j=mongodb_res[0],
            mongodb_ram_consumption_j=mongodb_res[1],
            mongodb_total_consumption_j=mongodb_res[2],
            mongodb_time_taken=mongodb_res[3],
            couchbase_cpu_consumption_j=couchbase_res[0],
            couchbase_ram_consumption_j=couchbase_res[1],
            couchbase_total_consumption_j=couchbase_res[2],
            couchbase_time_taken=couchbase_res[3],
            sorted_total=sorted_total,
            len=len(sorted_total),
        )
    else:
        return render_template("upload.html")


@app.route("/query_results")
def execute_query():
    return render_template("query_home.html")


@app.route("/compare", methods=["POST", "GET"])
def compare():
    if request.method == "POST":
        mysql_username = request.form["mysql_username"]
        mysql_password = request.form["mysql_password"]
        mysql_db_name = request.form["mysql_db_name"]
        mysql_query = request.form["mysql_query"]
        postgresql_username = request.form["postgresql_username"]
        postgresql_password = request.form["postgresql_password"]
        postgresql_db_name = request.form["postgresql_db_name"]
        postgresql_query = request.form["postgresql_query"]
        mongodb_db_name = request.form["mongodb_db_name"]
        mongodb_query = request.form["mongodb_query"]
        couchbase_username = request.form["couchbase_username"]
        couchbase_password = request.form["couchbase_password"]
        couchbase_bucket_name = request.form["couchbase_bucket_name"]
        couchbase_query = request.form["couchbase_query"]

        time.sleep(1)
        mysql_res = execute_mysql_query(
            mysql_query, mysql_username, mysql_password, mysql_db_name
        )
        time.sleep(1)
        postgresql_res = execute_postgreSQL_query(
            postgresql_query,
            postgresql_username,
            postgresql_password,
            postgresql_db_name,
        )
        time.sleep(1)
        mongodb_res = execute_mongodb_query(mongodb_query, mongodb_db_name)
        time.sleep(1)
        couchbase_res = execute_couchbase_query(
            couchbase_query,
            couchbase_username,
            couchbase_password,
            couchbase_bucket_name,
        )
        time.sleep(1)

        databases = ["MySQl", "PostgreSQl", "Mongodb", "Couchbase"]
        eff_total_consumption = [
            mysql_res[2],
            postgresql_res[2],
            mongodb_res[2],
            couchbase_res[2],
        ]
        databases_total_dict = {
            index: name for index, name in zip(eff_total_consumption, databases)
        }
        sorted_total = [
            databases_total_dict[i]
            for i in sorted(databases_total_dict, key=lambda x: Decimal(x))
        ]

        return render_template(
            "compare_result.html",
            mysql_cpu_consumption_j=mysql_res[0],
            mysql_ram_consumption_j=mysql_res[1],
            mysql_total_consumption_j=mysql_res[2],
            mysql_time_taken=mysql_res[3],
            postgresql_cpu_consumption_j=postgresql_res[0],
            postgresql_ram_consumption_j=postgresql_res[1],
            postgresql_total_consumption_j=postgresql_res[2],
            postgresql_time_taken=postgresql_res[3],
            mongodb_cpu_consumption_j=mongodb_res[0],
            mongodb_ram_consumption_j=mongodb_res[1],
            mongodb_total_consumption_j=mongodb_res[2],
            mongodb_time_taken=mongodb_res[3],
            couchbase_cpu_consumption_j=couchbase_res[0],
            couchbase_ram_consumption_j=couchbase_res[1],
            couchbase_total_consumption_j=couchbase_res[2],
            couchbase_time_taken=couchbase_res[3],
            sorted_total=sorted_total,
            len=len(sorted_total),
        )
    elif request.method == "GET":
        return render_template("choice.html")


@app.route("/get_single_query_details", methods=["POST"])
def get_single_query_details():
    try:
        selected_option = request.form.get("database_type")
        if selected_option == "mongodb":
            database_name = request.form["database_name"]
            query = request.form["query"]
            res = execute_mongodb_query(query, database_name)
        elif selected_option == "mysql":
            username = request.form["username"]
            password = request.form["password"]
            database_name = request.form["database_name"]
            query = request.form["query"]
            res = execute_mysql_query(query, username, password, database_name)
        elif selected_option == "postgresql":
            username = request.form["username"]
            password = request.form["password"]
            database_name = request.form["database_name"]
            query = request.form["query"]
            res = execute_postgreSQL_query(query, username, password, database_name)
        elif selected_option == "couchbase":
            username = request.form["username"]
            password = request.form["password"]
            database_name = request.form["database_name"]
            query = request.form["query"]
            res = execute_couchbase_query(query, username, password, database_name)

        return render_template(
            "dbJoules_result.html",
            cpu_consumption=res[0],
            ram_consumption=res[1],
            total_consumption=res[2],
            time_taken=res[3],
        )
    except Exception as e:
        error = "Please enter valid credentials or query"
        return render_template("query_home.html", error=error)


def carbon_to_miles(kg_carbon):
    f_carbon = float(kg_carbon)
    res = 4.09 * 10 ** (-7) * f_carbon
    return "{:.2e}".format(res)


def carbon_to_tv(kg_carbon):
    f_carbon = float(kg_carbon)
    res = f_carbon * (1 / 0.097) * 60
    return "{:.2e}".format(res)


def execute_mysql_query(query, db_user, db_password, db_name):
    connection = mysql.connector.connect(
        user=db_user, password=db_password, host="localhost", database=db_name
    )
    cursor = connection.cursor()
    obj = Tracker()
    obj.start()
    res = []
    cursor.execute(query)
    splitted_query = query.upper().split()
    if (
        splitted_query[0] == "DELETE"
        or splitted_query[0] == "UPDATE"
        or (splitted_query[0] == "INSERT" and splitted_query[1] == "INTO")
    ):
        connection.commit()
        connection.close()
    else:
        result_set = cursor.fetchall()
        connection.close()
    obj.stop()
    res.append(round(obj.cpu_consumption(), 2))
    res.append(round(obj.ram_consumption(), 2))
    res.append(round(obj.consumption(), 2))
    res.append(round(float(obj.duration), 2))
    return res


def execute_postgreSQL_query(
    postgresql_query, postgresql_user, postgresql_password, postgresql_db_name
):
    connection = psycopg2.connect(
        host="localhost",
        database=postgresql_db_name,
        user=postgresql_user,
        password=postgresql_password,
    )
    cursor = connection.cursor()
    obj = Tracker()
    obj.start()
    res = []
    cursor.execute(postgresql_query)
    connection.commit()
    cursor.close()
    connection.close()
    obj.stop()
    res.append(round(obj.cpu_consumption(), 2))
    res.append(round(obj.ram_consumption(), 2))
    res.append(round(obj.consumption(), 2))
    res.append(round(float(obj.duration), 2))
    return res


def execute_couchbase_query(
    couchbase_query, couchbase_username, couchbase_password, couchbase_bucket_name
):
    auth = PasswordAuthenticator(couchbase_username, couchbase_password)
    cluster = Cluster.connect("couchbase://localhost", ClusterOptions(auth))
    cb = cluster.bucket(couchbase_bucket_name)
    cb_coll = cb.default_collection()

    key_start = couchbase_query.find('VALUES("') + len('VALUES("')
    key_end = couchbase_query.find('",', key_start)
    key = couchbase_query[key_start:key_end]
    start_index = couchbase_query.find("{")
    end_index = couchbase_query.rfind("}")
    data = couchbase_query[start_index : end_index + 1]

    if "INSERT INTO" in couchbase_query and "VALUES" in couchbase_query:
        try:
            if cb_coll.get(key).success:
                key = generate_unique_key()
        except DocumentNotFoundException:
            pass
        pattern = r'VALUES\(".*?",\{(.+?)\}\);'
        couchbase_query = re.sub(
            pattern, f'VALUES("{key}",{{\\1}});', couchbase_query, flags=re.IGNORECASE
        )

    obj = Tracker()
    obj.start()
    res = []
    if "INSERT INTO" in couchbase_query:
        document = json.loads(data)
        cb_coll.upsert(key, document)
    else:
        result = cluster.query(couchbase_query)
    obj.stop()
    res.append(round(obj.cpu_consumption(), 2))
    res.append(round(obj.ram_consumption(), 2))
    res.append(round(obj.consumption(), 2))
    res.append(round(float(obj.duration), 2))
    return res


def execute_mongodb_query(query, db_name):
    client = MongoClient("mongodb://localhost:27017/")
    obj = Tracker()
    obj.start()
    res = []
    splitted_query = query.split(".")
    collection_name = splitted_query[1]
    db = client[db_name]
    collection = db[collection_name]
    query_field = splitted_query[2]
    additional_funcs = []
    if len(splitted_query) > 3:
        for i in range(3, len(splitted_query)):
            additional_funcs.append(splitted_query[i])

    if "insertOne" in query_field:
        query_doc = query_field.split("insertOne(")[1].split(")")[0]
        arg_dict = eval(query_doc)
        result = collection.insert_one(arg_dict)
    elif "insertMany" in query_field:
        query_doc = query_field.split("insertMany(")[1].split(")")[0]
        arg_dict = eval(query_doc)
        result = collection.insert_many(arg_dict)
    elif "findOne" in query_field:
        query_doc = query_field.split("findOne(")[1].split(")")[0]
        arg_dict = eval(query_doc)
        result = collection.find_one(arg_dict)
    elif "find" in query_field:
        query_doc = query_field.split("find(")[1].split(")")[0]
        if query_doc == "":
            result = collection.find()
        else:
            split_quer_doc = query_doc.split(",")
            arg_dict = [eval(q) for q in split_quer_doc]
            result = collection.find(*arg_dict)
    elif "updateOne" in query_field:
        query_doc = query_field.split("updateOne(")[1].split(")")[0]
        split_quer_doc = query_doc.split(",")
        arg_dict = [eval(q) for q in split_quer_doc]
        result = collection.update_one(*arg_dict)
    elif "updateMany" in query_field:
        query_doc = query_field.split("updateMany(")[1].split(")")[0]
        split_quer_doc = query_doc.split(",")
        arg_dict = [eval(q) for q in split_quer_doc]
        result = collection.update_many(*arg_dict)
    elif "deleteOne" in query_field:
        query_doc = query_field.split("deleteOne(")[1].split(")")[0]
        split_quer_doc = query_doc.split(",")
        arg_dict = [eval(q) for q in split_quer_doc]
        result = collection.delete_one(*arg_dict)
    elif "deleteMany" in query_field:
        query_doc = query_field.split("deleteMany(")[1].split(")")[0]
        split_quer_doc = query_doc.split(",")
        arg_dict = [eval(q) for q in split_quer_doc]
        result = collection.delete_many(*arg_dict)

    client.close()
    obj.stop()
    res.append(round(obj.cpu_consumption(), 2))
    res.append(round(obj.ram_consumption(), 2))
    res.append(round(obj.consumption(), 2))
    res.append(round(float(obj.duration), 2))
    return res


# ======================================================
# MAIN
# ======================================================

# ======================================================
# MAIN
# ======================================================

if __name__ == "__main__":

    print("\n📊 Select Experiment to Run:\n")
    print("1 → Format vs Operation (Dataset Size)")
    print("2 → Geometry Complexity Experiment")
    print("3 → Spatial Index (With vs Without Index)")
    print("6 → Compression Codec Experiment")

    choice = input("\nEnter your choice (1/2/3/6): ").strip()

    formats = ["geojson", "shp", "gpkg", "parquet"]
    operations = ["SELECT", "INSERT", "UPDATE", "DELETE", "JOIN"]

    # ======================================================
    # OPTION 1: FORMAT EXPERIMENT
    # ======================================================
    if choice == "1":

        print("\n🚀 Running Spatial Format Energy Experiment\n")
        results = []

        for fmt in formats:
            file_path = f"data/sample.{fmt}"

            for op in operations:
                print(f"\n[FORMAT] {fmt.upper()} | {op}")

                try:
                    result = run_experiment(
                        run_format_operation, file_path, op, runs=30
                    )

                    # ---------------------------------
                    # CREATE CLEAN ROW FOR CSV
                    # ---------------------------------
                    row = {
                        "Dataset": "Bosnia",   # change accordingly
                        "Format": fmt.upper(),
                        "Operation": op,
                        "Energy": result["mean_energy"],
                        "Time": result["mean_time"],
                    }

                    results.append(row)

                    print(f"  mean_time:   {result['mean_time']:.4f}s")
                    print(f"  mean_energy: {result['mean_energy']:.6f}J")

                except Exception as e:
                    print("Error:", e)

        os.makedirs("results", exist_ok=True)

        pd.DataFrame(results).to_csv(
            "d1_plots/format_experiment.csv",
            mode="a",
            header=not os.path.exists("d1_plots/format_experiment.csv"),
            index=False
        )

        print("\n✅ Results saved to d1_plots/format_experiment.csv")

    # ======================================================
    # OPTION 2: GEOMETRY EXPERIMENT
    # ======================================================
    elif choice == "2":

        print("\n🔥 Running Geometry Complexity Experiment\n")

        geometry_types = [
            "points",
            "lines",
            "simple_polygons",
            "complex_polygons"
        ]

        results = []

        for geom in geometry_types:

            print(f"\n{'='*20} {geom.upper()} {'='*20}")

            for fmt in formats:

                file_path = f"geom_data/{geom}.{fmt}"

                for op in operations:

                    print(f"\n[GEOMETRY] {geom.upper()} → {fmt.upper()} → {op}")

                    try:

                        result = run_experiment(
                            run_geometry_operation,
                            file_path,
                            op,
                            runs=30
                        )

                        # -----------------------------------
                        # STORE EXACT CSV FORMAT
                        # -----------------------------------
                        row = {
                            "operation": op,
                            "file": file_path,
                            "mean_time": result["mean_time"],
                            "std_time": result["std_time"],
                            "mean_energy": result["mean_energy"],
                            "std_energy": result["std_energy"],
                            "geometry": geom,
                            "format": fmt
                        }

                        results.append(row)

                        print(f"  mean_time:   {result['mean_time']:.4f}s")
                        print(f"  std_time:    {result['std_time']:.4f}s")
                        print(f"  mean_energy: {result['mean_energy']:.6f}J")
                        print(f"  std_energy:  {result['std_energy']:.6f}J")

                    except Exception as e:
                        print("Error:", e)

        # -----------------------------------
        # SAVE CSV
        # -----------------------------------
        os.makedirs("d2_plots", exist_ok=True)

        pd.DataFrame(results).to_csv(
            "d2_plots/geometry_experiments.csv",
            index=False
        )

        print("\n✅ Results saved to d2_plots/geometry_experiments.csv")

    # ======================================================
    # OPTION 3: INDEX EXPERIMENT
    # ======================================================
    elif choice == "3":

        print("\n⚡ Running Spatial Index Experiment\n")

        index_operations = ["SELECT", "JOIN"]
        cases = {
            "NO_INDEX": "index_data/sample_noindex.gpkg",
            "WITH_INDEX": "index_data/sample_index.gpkg",
        }

        results = []

        for case, file_path in cases.items():
            print(f"\n{'='*20} {case} {'='*20}")

            for op in index_operations:
                print(f"\n[{case}] → {op}")

                try:

                    # wrapper to pass use_index flag
                    def wrapper(fp, oper):
                        return run_index_operation(
                            fp, oper, use_index=(case == "WITH_INDEX")
                        )

                    result = run_experiment(wrapper, file_path, op, runs=30)

                    result["case"] = case
                    result["operation"] = op
                    results.append(result)

                    print(f"  mean_time:   {result['mean_time']:.4f}s")
                    print(f"  mean_energy: {result['mean_energy']:.6f}J")

                except Exception as e:
                    print("Error:", e)

        os.makedirs("results", exist_ok=True)

        pd.DataFrame(results).to_csv(
            "d3_plots/index_experiment.csv",
            index=False
        )

        print("\n✅ Results saved to d3_plots/index_experiment.csv")

    # ======================================================
    # OPTION 6: COMPRESSION EXPERIMENT
    # ======================================================
    elif choice == "6":

        print("\n🧪 Running Compression Codec Experiment\n")

        compression_cases = {
            "PARQUET_UNCOMPRESSED": "compression_data/parquet_uncompressed.parquet",
            "PARQUET_SNAPPY": "compression_data/parquet_snappy.parquet",
            "PARQUET_ZSTD": "compression_data/parquet_zstd.parquet",
            "GEOJSON_NORMAL": "compression_data/geojson_normal.geojson",
            "GEOJSON_GZIP": "compression_data/geojson_gzip.geojson.gz",
            "GPKG_NORMAL": "compression_data/normal_gpkg.gpkg",
            "GPKG_SIMPLIFIED": "compression_data/simplified_gpkg.gpkg",
        }

        results = []

        for comp, file_path in compression_cases.items():
            print(f"\n{'='*20} {comp} {'='*20}")

            for op in operations:
                print(f"\n[{comp}] → {op}")

                try:
                    result = run_experiment(
                        run_compression_operation,
                        file_path,
                        op,
                        runs=30
                    )

                    result["compression"] = comp
                    result["operation"] = op
                    results.append(result)

                    print(f"  mean_time:   {result['mean_time']:.4f}s")
                    print(f"  mean_energy: {result['mean_energy']:.6f}J")

                except Exception as e:
                    print("Error:", e)

        os.makedirs("results", exist_ok=True)

        pd.DataFrame(results).to_csv(
            "d6_plots/compression_experiment.csv",
            index=False
        )

        print("\n✅ Results saved to d6_plots/compression_experiment.csv")

    # ======================================================
    # INVALID INPUT
    # ======================================================
    else:
        print("\n❌ Invalid choice. Please run again.")