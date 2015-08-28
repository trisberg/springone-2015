cloud-hdfs-writer
=================

Spring Boot app for ingesting stream data to HDFS file. Stream is created using Spring Cloud Stream project.

### Build with:

    mvn clean package

### Start Hadoop

Follow instructions for the `SpringOne-2015-Edition` here [https://github.com/trisberg/hadoop-install#clone-this-repository-and-pick-a-branch-to-use](https://github.com/trisberg/hadoop-install#clone-this-repository-and-pick-a-branch-to-use)

### Run local with:

##### start sink with:
    java -jar target/cloud-hdfs-writer-0.0.1-SNAPSHOT.jar

##### start source with:
    docker run --add-host=borneo:$(grep 'borneo' /etc/hosts | awk '{print $1}') -p 8080:8080 -e MODULES=org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT -e spring_redis_host=borneo -e spring_cloud_stream_bindings_output=hdfs.data springcloud/stream-module-launcher


