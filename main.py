import json
import os
from git import Repo
import pandas as pd
from pymongo import MongoClient
import asyncio

client = MongoClient()
# Access a specific database (create if not exists)
db = client.my_database
# Access a specific db_collection (create if not exists)
db_collection = db.db_collection
# create a new collection
transformed_collection = db.transformed_collection
# create a new collection
raw_collection = db.raw_collection
# Define the path to clone the repository
filepath = "/app"

async def clone_repo():
    """
    Clone a repository from a given URL.

    Returns:
        str: A message indicating the success of the cloning process.
    """
    # Clone the repository
    repo = Repo.clone_from("https://github.com/emisgroup/exa-data-eng-assessment.git", filepath)
    return print("Repo cloned successfully")

async def insert_data():
    """
    Insert data from JSON files into the raw_collection.

    This function iterates over all the JSON files in the '/app/data' directory,
    reads each file, converts the data into a dictionary, and inserts the records
    into the raw_collection.

    Returns:
        None
    """
    # Insert data into the raw_collection
    for file in os.listdir("/app/data"):
       # Check if the file is a json file
       if file.endswith(".json"):
           # Read the json file
           dataframe = pd.read_json("/app/data/" + file)
           # Convert the dataframe to a dictionary
           records = json.loads(dataframe.T.to_json()).values()
    # Insert the records into the raw_collection
    raw_collection.insert_many(records)
    return print("Data inserted successfully")

async def transform_data():
    """
    Transforms the data in the raw_collection and inserts it into the transformed_collection.

    Returns:
        None
    """
    data = raw_collection.find()
    dataframe = pd.DataFrame(data)
    dataframe['resource'] = dataframe['entry'].apply(lambda x: x['resource']['resourceType'])
    dataframe['billablePeriod_start'] = dataframe['entry'].apply(lambda x: x.get('resource', {}).get('billablePeriod', {}).get('start'))
    dataframe['billablePeriod_end'] = dataframe['entry'].apply(lambda x: x.get('resource', {}).get('billablePeriod', {}).get('end')) 
    dataframe['insurance'] = dataframe['entry'].apply(lambda x: x.get('resource',{}).get('insurance'))
    dataframe['patient'] = dataframe['entry'].apply(lambda x: x.get('resource',{}).get('patient',{}))
    dataframe['status'] = dataframe['entry'].apply(lambda x: x.get('resource',{}).get('status'))
    dataframe = dataframe[['_id', 'type', 'resourceType','resource','billablePeriod_start', 'billablePeriod_end','insurance','patient','status', 'entry']]
    dataframe.drop('entry', axis=1, inplace=True)
    transformed_collection.insert_many(dataframe.to_dict('records'))
    return print("Data transformed successfully")

async def pipeline_setup():
    """
    Sets up the pipeline by performing the following steps:
    1. Clones the repository.
    2. Inserts data.
    3. Transforms data.
    
    Prints a success message when all steps are completed.
    """
    await clone_repo()
    await insert_data()
    await transform_data()
    print("Repo cloned, data inserted and transformed successfully")

if __name__ == '__main__':
    asyncio.run(pipeline_setup())