# sensor-failure

*interrupt detection in large sensor networks*

#### OVERVIEW

Event stream interruption is a common occurrence in sensor networks. We monitor a large, globally-distributed sensor network in realtime and store blocks of the *preceeding* event stream for later analysis.     

![basic flow](img/buoy-flow.png) 

The implementation uses Spark Arbitrary Stateful Streaming to consume from Kafka and stores results in Apache ORC.

#### BACKGROUND

[NOAA](https://www.noaa.gov/) manages a world-wide network of weather stations under auspices of the [National Data Buoy Center](https://www.ndbc.noaa.gov/). 

Stations are land-based, moored, and floating.

![land based](img/sbio1.png)
![moored](img/image003.png)
![floating](img/12m1.png)

Stations supply near-surface and underwater sensor arrays and report at fixed intervals or continuously. 

Data are transmitted via [GOES](https://en.wikipedia.org/wiki/Geostationary_Operational_Environmental_Satellite) or [Iridium](https://en.wikipedia.org/wiki/Iridium_satellite_constellation) satellite networks to a ground facility in Wallops Island, Virginia. Feeds are aggregated hourly and published to a publicly-accessible web-share location.

![sensor data flow](img/NDBC-dataflow.png)

NDBC's realtime [web-share](https://www.ndbc.noaa.gov/data/realtime2/) is updated hourly - "usually by 15 minutes after the hour". Data files store a 45-day history for a given station and are updated at the *top* of the file. 

For example:

![topoffile](img/head.png)

NDBC supplements with feeds from assets outside of its direct jurisdiction, including ship observations.

#### RESULTS

This trace shows sensor interrupts from weather station [NPXN6](https://www.ndbc.noaa.gov/station_page.php?station=npxn6): As the Stream is processed, the highlighted feed shows a pressure sensor sending a signal that goes silent.

![interrupts](img/two-interrupts.png)

Each time any monitored sensor stops sending a signal, a block of 16 records is processed by our streaming sinks.

The depicted example is a dev view, and uses a simple [ForeachWriter](src/main/scala/com/ubiquibit/buoy/jobs/InterruptWriter.scala) to writes records to disk.

In the actual implementation, we output to the Apache ORC format. (See: [Output](#output) for more info)

#### IMPLEMENTATION

##### WxStream

The WxStream Spark Application is where the action happens. 

WxStream consumes from one or more Station [Kafka topics](src/main/scala/com/ubiquibit/buoy/jobs/WxStream.scala#L71) and then *splays out* execution using [flatMapGroupWithState](src/main/scala/com/ubiquibit/buoy/jobs/WxStream.scala#L83).

StationInterrupts' [updateInterrupts function](src/main/scala/com/ubiquibit/buoy/jobs/StationInterrupts.scala#L44) calculates "interrupts" and "onlineAgain" for each consecutive pair of records:

![core processing](img/processed.png)

Together with Spark's arbitrary stateful processing engine, the algorithm is capable of handling out-of-order event arrivals, record timeouts (to preserve memory), and failure resilience. The design scales out to support arbitrarily large networks.

##### Input

Kafka can be fed in a large batch or slowly, over time. 

Use `InitKafkaImpl` to batch load historical data. It needs to be run one time per station. (Instructions below.) 

Use `LiveKafkaFeeder` to keep Kafka up-to-date. It checks the web-share for recent changes. (Instructions below.)

You may use either one, but it's probably best to start with InitKafkaImpl even in the case that you want to run the live feeder.

##### Output

ORC is a standalone Apache project and is supported natively by Apache Spark. It provides columnar storage (like Parquet) and Snappy compression. The format is widely used in Big Data and Machine Learning. 

Here is a partial output of the hive command, executed against the output directory:

![orc output](img/OrcOutput.png)

(It is interesting to note that a number of chanels *never* report data.)

At this point, our Streaming Data have been ETL'd and are ready for OLAP!

#### HOWTO

The remainder of this document provides steps to do it yourself.  

##### Pre-requisites for howto

These tools work to run the demo

- Apache Spark (2.4.0-bin-hadoop-2.7) [download](https://spark.apache.org/downloads.html) - [instructions](https://spark.apache.org/docs/latest/)
- Apache Kafka (2.12-2.1.1) [download](https://kafka.apache.org/downloads) - [instructions](https://kafka.apache.org/quickstart)
- sbt (1.2.7) [download](https://www.scala-sbt.org/download.html)
- scala (2.11.12) [download](https://www.scala-lang.org/download/)
- Redis (run via Docker) (instructions below)

#####  download buoy data 

```bash 
# from within `BASEDIR`...
% mkdir data && cd data ;
% wget -r -np -R "index.html*" https://www.ndbc.noaa.gov/data/realtime2/ ; 

# wait...

```

You should have data from ~950 WxStations reporting ~17 different output formats. 

```bash
# Adcp files 29
# Adcp2 files 39
# Cwind files 75
# Dart files 45
# DataSpec files 152
# Drift files 8
# Hkp files 2
# Ocean files 178
# Rain files 36
# Spec files 312
# Srad files 17
# Supl files 65
# Swdir files 149
# Swr1 files 149
# Swr2 files 149
# Text files 799
```

##### Clone & Build

```bash

% git clone https://github.com/jasonnerothin/sensor-failure.git
% cd sensor-failure 
% git checkout data-processing
```
- edit [application.properties](src/main/resources/application.properties)
```bash
% sbt assembly

...

[info] Packaging /Users/jason/scratch/sensor-failure/target/scala-2.11/sensorfailure-assembly-1.0.jar ...
[info] Done packaging.
[success] Total time: 31 s, completed Apr 1, 2019 4:05:45 PM
[IJ]sbt:sensorfailure> 

```

##### Kafka & Redis

- Run Redis (on Docker)
````bash
# FROM `BASEDIR`...
cd bash
./start-redis.sh
````
- run [InitRedisImpl](src/main/scala/com/ubiquibit/buoy/jobs/setup/InitKafka.scala) 

- Install and start Kafka 

```bash 
# edit, then run...
./start-kafka.sh
./create-kafka-topics.sh

# wait...
```

1. Batch load using [bash/Load-SE-USA-buoys-to-Kafka.sh](batch script) ~ follow instructions in script...

2. OR run [InitKafkaImpl](src/main/scala/com/ubiquibit/buoy/jobs/setup/InitKafka.scala) 

> It loads one (text) data feed per run from filesystem - recommend 3-4 runs for simple setup.

```bash

# before

./redis-login.sh

...

redis:6379> hmget "stationId:46082" "TXT"
1) "DOWNLOADED"

# Loads hard-coded Station 46082...

${SPARK_HOME}/bin/spark-submit --class "com.ubiquibit.buoy.jobs.setup.InitKafkaImpl" --master "spark://${SPARK_HOST}:7077" --deploy-mode cluster --executor-cores 2 --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" "/Users/jason/scratch/sensor-failure/target/scala-2.11/sensorfailure-assembly-1.0.jar"
19/04/02 09:06:23 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable

# OR loads a station of your choosing...

${SPARK_HOME}/bin/spark-submit --class "com.ubiquibit.buoy.jobs.setup.InitKafkaImpl" --master "spark://${SPARK_HOST}:7077" --deploy-mode cluster --executor-cores 2 --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" "/Users/jason/scratch/sensor-failure/Target/scala-2.11/sensorfailure-assembly-1.0.jar" "BZST2"

# after

./redis-login.sh

...

redis:6379> hmget "stationId:46082" "TXT"
1) "KAFKALOADED"
```

##### Init WxStream

- run [StageFeeds](src/main/scala/com/ubiquibit/buoy/jobs/util/StageFeeds.scala)

> It writes a file to the staging directory that will later be used by [WxStream](src/main/scala/com/ubiquibit/buoy/jobs/WxStream.scala)

##### Run WxStream

```bash
${SPARK_HOME}/bin/spark-submit --class "com.ubiquibit.buoy.jobs.WxStream" --master "spark://${SPARK_HOST}:7077" --deploy-mode cluster --executor-cores 4 --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" "/Users/jason/scratch/sensor-failure/target/scala-2.11/sensorfailure-assembly-1.0.jar"
``` 

> check the driver's stdout log and [SparkUI](http://localhost:8080)

Note: WxStream console output shows up in the *driver* stdout. StationInterrupt and other debug logging shows up in the *application* stderr (if configured in `$SPARK_HOME/conf/log4.properties`)
