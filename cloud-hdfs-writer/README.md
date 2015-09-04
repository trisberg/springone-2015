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

##### download the time-source module with:
    wget http://repo.spring.io/milestone/org/springframework/cloud/stream/module/time-source/1.0.0.M1/time-source-1.0.0.M1-exec.jar

##### start source module::
    java -jar time-source-1.0.0.M1-exec.jar --server.port=8081 --spring.redis.host=borneo --spring.cloud.stream.bindings.output=hdfs.data

#### Run on Lattice with:

We need to have Lattice configured and running.

##### start redis with:
    ltc create redis redis -r

##### start sink with:
    ltc create hdfs springdeveloper/cloud-hdfs-writer --memory-mb=512 -e spring_profiles_active=cloud -e spring_hadoop_fsUri=hdfs://borneo:8020

##### start source using Docker (this starts a Spring XD v2 `time-source` module):
    ltc create time springcloud/stream-module-launcher --memory-mb=512 -e MODULES=org.springframework.cloud.stream.module:time-source:1.0.0.BUILD-SNAPSHOT -e spring_profiles_active=cloud -e spring_cloud_stream_bindings_output=hdfs.data

#### Run on Cloud Foundry with:

You need to have an account for Cloud Foundry and also a Hadoop cluster that Cloud Foundry can access over the network.

##### configure redis and hadoop services with (adjust to match your environment):
    cf create-service rediscloud 30mb redis
    cf create-user-provided-service hadoop -p '{"fs":{"defaultFS":"hdfs://10.0.0.1:8020"},"yarn":{"resourcemanager":{"host":"10.0.0.2","port":"8050"}}}'

##### start sink with:
    cf push springone-hdfs -p target/cloud-hdfs-writer-0.0.1-SNAPSHOT.jar --no-start
    cf bind-service springone-hdfs hadoop
    cf bind-service springone-hdfs redis
    cf start springone-hdfs

##### download the time-source module with:
    wget http://repo.spring.io/milestone/org/springframework/cloud/stream/module/time-source/1.0.0.M1/time-source-1.0.0.M1-exec.jar

##### start source with:
    cf push springone-time -p time-source-1.0.0.M1-exec.jar --no-start
    cf bind-service springone-time redis
    cf set-env springone-time SPRING_CLOUD_STREAM_BINDINGS_OUTPUT hdfs.data
    cf start springone-time


