from bson import json_util
import pandas as pd
from pymongo import MongoClient
from flask import Flask, jsonify, request,Response
from git import Repo
import os
import asyncio

app = Flask(__name__)
# Establish a connection to MongoDB (assuming it's running locally)
client = MongoClient()
# Access a specific database (create if not exists)
db = client.my_database
# Access a specific db_collection (create if not exists)
db_collection = db.db_collection
# Define the path to clone the repository
filepath = "/app"


@app.route('/', methods=['GET', 'POST'])
def hello_world():
    return 'Hello, World!'



@app.route('/get_data', methods=['GET'])
def get_data():
    data = db_collection.find()
    return Response(json_util.dumps(data), mimetype='application/json')


async def clone_repo():
    try:
        repo = Repo.clone_from("https://github.com/emisgroup/exa-data-eng-assessment.git", filepath)
        return jsonify({"message": "Repository cloned successfully"})
    except Exception as e:
        return jsonify({"error message": str(e)})

def insert_data():
    asyncio.run(clone_repo())
    for file in os.listdir("/app/data"):
       if file.endswith(".json"):
           data = json_util.loads(open("/app/data/" + file).read())
           db_collection.insert_one(data)
    return jsonify({"message": "Data inserted successfully"})

with app.app_context():
    insert_data()

if __name__ == "__main__":
    app.run(host='0.0.0.0')
