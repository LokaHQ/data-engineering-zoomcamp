# Spark with Docker
[Check on Docker Hub](https://hub.docker.com/r/apache/spark-py)

    docker build .
    docker run -d -p 8000:8000 --name <name_container> <docker_id>

# Spark Commands
    
    spark.version
    spark.range(1000 * 1000 * 1000).count()

# Docker Commands
    
    docker ps
    docker images
    docker exec -it <container_id> bash
