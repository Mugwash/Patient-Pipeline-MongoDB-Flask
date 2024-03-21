import json
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
transformed_collection = db.transformed_collection
# Define the path to clone the repository
filepath = "/app"


@app.route('/', methods=['GET', 'POST'])
def hello_world():
    return 'Hello, World!'



@app.route('/get_data', methods=['GET'])
def get_data():
    try:
        data = transformed_collection.find()
        dataframe = pd.DataFrame(data)
        #dataframe['resourceType'] = dataframe['entry'].apply(lambda x: x['resource']['resourceType'])
        #dataframe['resourceType'] = dataframe['entry'].apply(lambda x: x['resource']['resourceType'])
        html_table = dataframe.to_html()
        return html_table
    except Exception as e:
        return jsonify({"error message": str(e)})



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
           dataframe = pd.read_json("/app/data/" + file)
           records = json.loads(dataframe.T.to_json()).values()
    db_collection.insert_many(records)
    transform_data()
    return jsonify({"message": "Data inserted successfully"})

def transform_data():
    data = db_collection.find()
    dataframe = pd.DataFrame(data)
    dataframe['resourceType'] = dataframe['entry'].apply(lambda x: x['resource']['resourceType'])
    dataframe = dataframe[['_id', 'type', 'resourceType', 'entry']]
    dataframe.drop('entry', axis=1, inplace=True)
    transformed_collection.insert_many(dataframe.to_dict('records'))
    return jsonify({"message": "Data transformed successfully"})

with app.app_context():
    insert_data()

if __name__ == "__main__":
    app.run(host='0.0.0.0')
