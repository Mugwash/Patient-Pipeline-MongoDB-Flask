##Patient-Pipeline-MongoDB-Flask: A Comprehensive Data Pipeline SolutionOverview
The Patient-Pipeline-MongoDB-Flask project is a containerized data pipeline designed to extract data from a GitHub repository, process it, and present it in both raw and transformed formats. Here's a breakdown of the key components:
- Language: Python
- Data Layer: MongoDB
- Libraries Used: Pandas, Flask, Asyncio, Pymongo, Git, Pytest
Getting StartedTo run this project using Docker, follow these steps:
- Build the Docker Image:

docker build -t my-mongodb-flaskapp .

- Run the Docker Container:

docker run -p 5000:5000 -d my-mongodb-flaskapp

(Note: It's important to expose port 5000)
- Access the Flask App:
    - Visit http://localhost:5000/ to confirm that the Flask app is up and running.
    - Explore the following routes:
        - http://localhost:5000/get_raw_data to view the raw data inserted into the MongoDB database.
        - http://localhost:5000/get_transformed_data to see a table of the transformed data.
Note: Data may take a moment to appear due to the initial Git repo download.
Project StructureHere's an overview of the files in this project:
- app.py:
    - Contains the main code for the web application.
    - Retrieves raw data from a collection and converts it into an HTML table.
    - Handles exceptions during data retrieval and processing.
- main.py:
    - Sets up a data pipeline:
        - Clones a Git repository.
        - Inserts data from JSON files into a MongoDB collection (raw_collection).
        - Transforms the data using pandas and inserts it into another MongoDB collection (transformed_collection).
    - Executes the pipeline asynchronously using asyncio.
- Dockerfile:
    - Installs necessary dependencies in the container and exposes the required ports.
    - Sets up a Docker container based on the latest MongoDB version.
    - Copies Python files into the container.
    - Runs MongoDB and the Python scripts.
- test_app.py and test_main.py:
    - Contain pytest tests for the functions/methods in the respective Python files.
TestingTo run the tests, simply execute pytest in the terminal.
SummaryOverall, this solution performs well. While there may be a slight delay during initial Git repo download, it ensures a dynamic data source that doesn't require manual file downloads. The Flask app provides clear routes for viewing raw and transformed data in a tabular format, enhancing readability and usability.
