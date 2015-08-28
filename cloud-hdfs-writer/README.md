cloud-hdfs-writer
=================

Spring Boot app for ingesting stream data to HDFS file. Stream is created using Spring Cloud Stream project.

### Build with:

    mvn clean package

### Start Hadoop

Follow instructions for the `SpringOne-2015-Edition` here [https://github.com/trisberg/hadoop-install#clone-this-repository-and-pick-a-branch-to-use](https://github.com/trisberg/hadoop-install#clone-this-repository-and-pick-a-branch-to-use)

### Run local with:

We need to have Docker configured and running.

##### start sink with:
    java -jar target/cloud-hdfs-writer-0.0.1-SNAPSHOT.jar

##### start source using Docker (this starts a Spring XD v2 `time-source` module):
    docker run --add-host=borneo:$(grep 'borneo' /etc/hosts | awk '{print $1}') -p 8080:8080 -e MODULES=org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT -e spring_redis_host=borneo -e spring_cloud_stream_bindings_output=hdfs.data springcloud/stream-module-launcher

#### Run on Lattice with:

We need to have Lattice configured and running.

##### start redis with:
    ltc create redis redis -r

##### start sink with:
    ltc create hdfs springdeveloper/cloud-hdfs-writer --memory-mb=512 --timeout '4m0s' -e spring_profiles_active=cloud -e spring_hadoop_fsUri=hdfs://borneo:8020

##### start source using Docker (this starts a Spring XD v2 `time-source` module):
    ltc create time springcloud/stream-module-launcher --memory-mb=512 -e MODULES=org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT -e spring_profiles_active=cloud -e spring_cloud_stream_bindings_output=hdfs.data
