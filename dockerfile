FROM mongo:latest
# Set environment variables
ENV MONGO_INITDB_ROOT_USERNAME=myuser
ENV MONGO_INITDB_ROOT_PASSWORD=mypassword
RUN apt-get update && apt-get install -y python3 python3-pip
RUN apt-get update && apt-get install -y git
RUN pip install pymongo
RUN pip3 install flask
RUN pip install GitPython
RUN pip install pandas
RUN pip install pytest
COPY app.py /app.py
COPY main.py /main.py
# Expose port
EXPOSE 27017
EXPOSE 5000
WORKDIR /app
CMD mongod & python3 /main.py & python3 /app.py
