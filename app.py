from pymongo import MongoClient
from flask import Flask, jsonify, request
from git import Repo
import os
app = Flask(__name__)
# Establish a connection to MongoDB (assuming it's running locally)
client = MongoClient()

# Access a specific database (create if not exists)
db = client.my_database

# Access a specific collection (create if not exists)
my_collection = db.my_collection



@app.route('/')
def hello():
    return 'Hello, Flask World!'

@app.route('/clone_repo', methods=['GET', 'POST'])
def clone_repo():
      try:
        repo = Repo.clone_from("https://github.com/emisgroup/exa-data-eng-assessment.git", "/app")
        return jsonify({"message": "Repository cloned successfully"})
      except:
        return jsonify({"error message": "Repository failed to clone"})
      

    
@app.route('/create_database', methods=['POST'])
def create_database():
        db = client.my_database
        # Access a specific collection (create if not exists)
        my_collection = db.my_collection
        return jsonify({"message": f"Database '{db.name}' created successfully"})


@app.route('/add_user', methods=['POST'])
def add_user():
    user_data = request.get_json()  # Assuming JSON input
    new_document = {
        "name": user_data.get("name"),
        "age": user_data.get("age"),
        "email": user_data.get("email")
    }
    my_collection.insert_one(new_document)
    return jsonify({"message": "User added successfully"})

@app.route('/close_app', methods=['POST'])
def close_app():
        client.close()
        return jsonify({"message": "App closed successfully"})

# Query the inserted document
result = my_collection.find_one({"name": "Alice"})
print("Inserted document:", result)

# Close the connection
if __name__ == "__main__":
    app.run(host='0.0.0.0')