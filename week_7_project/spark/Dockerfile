# First-time build can take upto 10 mins.

FROM jupyter/pyspark-notebook:latest

USER root
#Basics
RUN sudo apt-get update
RUN sudo apt-get install apt-transport-https ca-certificates gnupg -y
RUN sudo apt install curl -y
#copy google token
RUN mkdir /home/jovyan/credentials
RUN mkdir /home/jovyan/scripts
COPY google_credentials.json /home/jovyan/credentials/google_credentials.json
COPY gcs-connector-hadoop3-latest.jar /home/jovyan/lib/gcs-connector-hadoop3-latest.jar
COPY spark-3.1-bigquery-0.27.1-preview.jar /home/jovyan/lib/spark-3.1-bigquery-0.27.1-preview.jar
#Setup gsutils
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | tee /usr/share/keyrings/cloud.google.gpg && apt-get update -y && apt-get install google-cloud-sdk -y
RUN gcloud auth activate-service-account --key-file /home/jovyan/credentials/google_credentials.json
RUN gcloud config set project zoomcamp-final
#setup google cloud storage for python
RUN pip install google-cloud-storage
RUN pip install google-cloud-bigquery
ENV GOOGLE_APPLICATION_CREDENTIALS="/home/jovyan/credentials/google_credentials.json"