## Week 2: Data Ingestion

### Launch Airflow Locally With Docker

Run the following commands:

    cd airflow
    docker-compose -f docker-compose-nofrills.yml build
    docker-compose -f docker-compose-nofrills.yml up

Open [AirFlow WebServer](http://localhost:8080/)

### Useful Docker Commands

Get Containers, Images & Volumes:

    docker ps
    docker images
    docker volume ls

Get container stats such as CPU & memory usage:

    docker stats $(docker ps -q)

Enter a container:

    docker exec -it <container_id> bash

Remove Containers, Images & Volumes:

    docker rm -f $(docker ps -a -q);
    docker rmi $(docker images);\n
    docker volume prune
