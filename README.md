SpringOne 2GX 2015
==================

Hadoop Workflows examples and slides for [SpringOne 2GX 2015](http://springone2gx.com/) in Washington, DC, September 14-17, 2015

Session:
--------
[Hadoop Workflows and Distributed YARN Apps using Spring technologies](https://2015.event.springone2gx.com/schedule/sessions/hadoop_workflows_and_distributed_yarn_apps_using_spring_technologies.html)


Demos:
------

* [boot-ingest](boot-ingest) Spring Boot app for HDFS ingestion
* [batch-hive2](batch-hive2) Spring Boot app for running HiveServer2 batch tasklet
* [batch-spark](batch-spark) Spring Boot app for running Spark on YARN batch tasklet
* [cloud-hdfs-writer](cloud-hdfs-writer) Spring Boot app for ingesting stream data to HDFS file. Stream is created using Spring Cloud Stream project.

For Windows users:
------------------

To access Hadoop running on Linux from Windows client you need a very minimal local Hadoop install.

Download [http://public-repo-1.hortonworks.com/hdp-win-alpha/winutils.exe](http://public-repo-1.hortonworks.com/hdp-win-alpha/winutils.exe) 

Place it in a bin directory under a Hadoop directory (C:\Hadoop\bin)

Then use: java -D"hadoop.home.dir=C:\Hadoop" -jar …

Tested the following demos on Windows 8.1:

* boot-ingest
* batch-hive2
* batch-spark
