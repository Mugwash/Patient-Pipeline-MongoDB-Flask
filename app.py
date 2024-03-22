import pandas as pd
from pymongo import MongoClient
from flask import Flask, jsonify
from main import raw_collection, transformed_collection

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def hello_world():
    """
    A simple route that returns a message indicating that the app is up and running.

    Returns:
        str: A message indicating that the app is up and running.
    """
    return 'App is up and running!'



@app.route('/get_transformed_data', methods=['GET'])
def get_data():
    """
    Retrieves transformed data from the database and returns it as an HTML table.

    Returns:
        str: HTML table containing the transformed data.
    
    Raises:
        Exception: If an error occurs while retrieving or processing the data.
    """
    try:
        # Retrieve the transformed data from the transformed_collection
        data = transformed_collection.find()
        # Convert the data to a dataframe
        dataframe = pd.DataFrame(data)
        # Convert the dataframe to an HTML table
        html_table = dataframe.to_html()
        return html_table
    except Exception as e:
        return jsonify({"error message": str(e)})
    
@app.route('/get_raw_data', methods=['GET'])
def get_raw_data():
    """
    Retrieves raw data from the raw_collection and returns it as an HTML table.

    Returns:
        str: HTML table containing the raw data.
    
    Raises:
        Exception: If an error occurs while retrieving or processing the data.
    """
    try:
        # Retrieve the raw data from the raw_collection
        data = raw_collection.find()
        # Convert the data to a dataframe
        dataframe = pd.DataFrame(data)
        # Convert the dataframe to an HTML table
        html_table = dataframe.to_html()
        return html_table
    except Exception as e:
        return jsonify({"error message": str(e)})
    

if __name__ == '__main__':
    app.run(host='0.0.0.0')

