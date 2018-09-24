# Flink Session Example

This is a collection of some simple examples of Apache Flink for teaching.

# Setting Up Kafka Locally
The examples in this repo will all be using Kafka as the source of the Stream.
To make it easy for people we will use Docker to install and run Kafka

### Set Up Kafka
The following command will download, install, and run a Kafka Container to your local.

```  
docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=127.0.0.1 --env ADVERTISED_PORT=9092 --name mykafka -d spotify/kafka
```

To see the container running you can use the docker ps cmd line function
```
docker ps
```
Then you should see something like the following output
```
   CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS                                            NAMES
   a7b9cf39eb05        spotify/kafka       "supervisord -n"    39 seconds ago      Up 38 seconds       0.0.0.0:2181->2181/tcp, 0.0.0.0:9092->9092/tcp   mykafka
```

### Create Kafka Topic
Now that we have Kafka running we need to set up a topic to use.  
The following command will log you into the container using the bash terminal and once there you can use the Kafka cmds 
to set up a topic

```
docker exec -it mykafka bash

cd /opt/kafka_2.11-0.10.1.0/bin

./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic flink_training
```

If you want to see the topic just use the following cmd
```
./kafka-topics.sh --list --zookeeper localhost:2181
```

If you want to see the messages going into our newly made topic, the following command will make a consumer on the 
terminal so you can see events in clear text. 
```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic flink_training --from-beginning
```

# Word Counts Examples

## Sending Messages to Kafka 

### Send Word Count Data to Kafka
Generate some word count data we are going to start a cmd line producer in a new terminal.
The following cmd lines will set up a producer for us to type in word to the terminal that will feed our word count exapmle

```
docker exec -it mykafka bash

cd /opt/kafka_2.11-0.10.1.0/bin

./kafka-console-producer.sh --broker-list localhost:9092 --topic flink_training
```

## Word Count Exmaples

### Simple Word Count Example
The most basic example is com.tmalaska.flinktraining.example.wordcount.SimpleWordCount

This example uses basic processing time and a count window.  To start it use the following parameters

```
localhost 9092 flink_training group_id tumbleTime
```

These parameters are 
- Kafka Broker Host IP
- Kafka Broker Port
- Kafka Topic
- Kafka Consumer Group
- The type of window: tumbleTime, slidingCount, slidingTime, count

### Trigger Evict Word Count Example
The most basic example is com.tmalaska.flinktraining.example.wordcount.TriggerEvictWordCount

This example shows how to use triggers and evict to control the window

```
localhost 9092 flink_training group_id
```

These parameters are 
- Kafka Broker Host IP
- Kafka Broker Port
- Kafka Topic
- Kafka Consumer Group

### Streaming SQL Example
The most basic example is com.tmalaska.flinktraining.example.wordcount.StreamingSQL

This example uses append only SQL to simpliy code

```
localhost 9092 flink_training group_id
```

These parameters are 
- Kafka Broker Host IP
- Kafka Broker Port
- Kafka Topic
- Kafka Consumer Group

### Map With State Word Count Example
The most basic example is com.tmalaska.flinktraining.example.wordcount.MapWithStateWordCount

This example shows us how to use the MapWithState method and the Keyed State Values

```
localhost 9092 flink_training group_id
```

These parameters are 
- Kafka Broker Host IP
- Kafka Broker Port
- Kafka Topic
- Kafka Consumer Group



## Generated Session Messages to Send to Kafka
We will be sending messages to Kafka in a JSON format.  We are using JSON because it is human readable.
The Goal here is teaching and not performance.  

We will be generating out JSON from the HeartBeat Case Class.

```
case class HeartBeat(entityId:String, eventTime:Long)
```

### Run Message Session Generator
To generate messages and send them to Kafka we can use the SessionKafkaProducer object in the 
com.tmalaska.flinktraining.example.session package.

We will need to give SessionKafkaProducer the following parameters
```
localhost 9092 flink_training 10 100 1000 4
```

The parameters are as follows:
- Host of the Kafka Broker
- Port of the Kafka Broker
- Name of the Topic we want to write too
- The number of entities ID to be sent
- The max number of messages that could be sent for a given entity
- The wait time between loops of sending out messages for each entity
- N in 1/N, where these are the odds to not send a message for a entity on a given loop

**Whats with the Entity Count and the 1/N missing stuff?**
This generator is all about session generation.  We are going to have flink jobs that do sessionization.
So we need a generator that will generate data that can be sessionizated.  That means we need to have messages
related to a given entity that are close enough to continue a session and far apart enough to close a session and 
start a new one.

##Run Session Examples

### Streaming Session Example
The most advanced example in this project is the sessionization example.
That is because this example uses the processFunction command, which gives you the most detailed 
control of or in memory window.

The class is com.tmalaska.flinktraining.example.session.StreamingSessionExample

```
localhost 9092 flink_training group_id 10000
```

These parameters are 
- Kafka Broker Host IP
- Kafka Broker Port
- Kafka Topic
- Kafka Consumer Group
- milliseconds for a session gap

### Event Time Example
Lastly is a event time example with our session data.

Use com.tmalaska.flinktrainingBeatExample

```
localhost 9092 flink_training group_id
```

These parameters are 
- Kafka Broker Host IP
- Kafka Broker Port
- Kafka Topic
- Kafka Consumer Group

