
# Kafka Connect for Rockset

Kafka Connect for [Rockset](https://rockset.com/) is a [Kafka Connect Sink](https://docs.confluent.io/current/connect/index.html). This connector helps you load your data from Kafka Streams into Rockset collections through the [Rockset Streaming Write API](https://docs.rockset.com/source-rockset-streaming-write/). **Only valid JSON documents can be read from Kafka Streams and written to Rockset collections by Kafka Connect for Rockset.**


## Requirements

1. Kafka version 1.0.0+.

2. Java 8+.

3. An active [Rockset account](https://docs.rockset.com/overview/).  

## Build

1. Clone the repo from https://github.com/rockset/kafka-connect-rockset

2. Verify that Java8 JRE or JDK is installed.

3. Run `mvn package`. This will build the jar in the /target directory. The name will be `kafka-connect-rockset-[VERSION]-SNAPSHOT-jar-with-dependencies.jar`.  

## Usage

1.  [Start](https://kafka.apache.org/quickstart) your Kafka cluster and confirm it is running.

2. Kafka Connect can be run in standalone or distributed mode. In both modes, there is one configuration file that controls Kafka connect and a separate set of configuration for Rockset specific parameters. Depending on whether you are trying to run locally (standalone) or distributed, you will want to edit the appropriate configuration file -  `$KAFKA_HOME/config/connect-standalone.properties` or `$KAFKA_HOME/config/connect-distributed.properties` respectively.
2. In the config file mentioned above, adjust the values as shown below. For more information on installing Kafka Connect plugins please refer to the [Confluent Documentation.](https://docs.confluent.io/current/connect/userguide.html#id3)

| Name | Value |
|-------- | ---------------------------- |
| bootstrap.servers | `<list-of-kafka-brokers>` |
| plugin.path | `/path/to/rockset/sink/connector.jar` |

3. In addition to this, if you are dealing with JSON files in your stream already, you can turn off schema enforcement and conversion that Kafka connect provides by setting the following properties in the config file.

| Name | Value |
|-------- | ---------------------------- |
| key.converter | org.apache.kafka.connect.storage.StringConverter  |
| value.converter | org.apache.kafka.connect.storage.StringConverter |
| key.converter.schemas.enable | false |
| value.converter.schemas.enable | false |

3. Place the jar file created by `mvn package` (``kafka-connect-rockset-[VERSION]-SNAPSHOT-jar-with-dependencies.jar``) in or under the location specified in `plugin.path`
4. You can choose to run Kafka Connect in [standalone or distributed](https://docs.confluent.io/current/connect/userguide.html#standalone-vs-distributed) mode. To run it in standalone mode, modify the configuration file in the config/ directory and set the required parameters (see below). When running in distributed mode, you will use the REST API to set the same parameters and not the configuration file.
5. Run `$KAFKA_HOME/bin/connect-standalone.sh $KAFKA_HOME/config/connect-standalone.properties ./config/connect-rockset-sink.properties` to start Kafka Connect with Rockset configured. This is sufficient for testing and should let you run a local Kafka Connect worker that uses the configuration provided in `./config/connect-rockset-sink.properties` to write JSON documents from Kafka to Rockset.
6. Alternately, if you're running in distributed mode, you'll run: `$KAFKA_HOME/bin/connect-distributed.sh $KAFKA_HOME/config/connect-distributed.properties` to start Kafka Connect. You can then configure Kafka Connect using the REST API.
  
```
curl -i http://localhost:8083/connectors -H "Content-Type: application/json" -X POST -d '{
    "name": "rockset-sink",
    "config":{
      "connector.class": "rockset.RocksetSinkConnector",
      "tasks.max": "20",
      "rockset.task.threads": "5",
      "topics": "<your-kafka-topics separated by commas>",
      "rockset.collection": "<your-rockset-collection>",
      "rockset.apikey": "<your-api-key>",
      "rockset.apiserver.url": "https://api.rs2.usw2.rockset.com"
    }
}'
```

7. Use the following commands to check status, and manage connectors and tasks:
```

# List active connectors
curl http://localhost:8083/connectors

# Get rockset-sink connector info
curl http://localhost:8083/connectors/rockset-sink

# Get rockset-sink connector config info
curl http://localhost:8083/connectors/rockset-sink/config

# Delete rockset-sink connector
curl http://localhost:8083/connectors/rockset-sink -X DELETE

# Get rockset-sink connector task info
curl http://localhost:8083/connectors/rockset-sink/tasks

```

See the [the Confluent doucumentation](https://docs.confluent.io/current/connect/managing.html#common-rest-examples) for more REST examples.

## Configuration

### Parameters

#### Required Parameters

| Name | Description | Default Value |
|-------- |----------------------------|-----------------------|
|`name` | Connector name. A consumer group with this name will be created with tasks to be distributed evenly across the connector cluster nodes.|
| `connector.class` | The Java class used for executing the connector logic. |`rockset.RocksetSinkConnector`|
| `tasks.max` | The number of tasks generated to handle data collection jobs in parallel. The tasks will be spread evenly across all Rockset Kafka Connector nodes.||
| `topics` | List of comma-separated Kafka topics that should be watched by this Rockset Kafka Connector.||
| `rockset.apiserver.url` | URL of the Rockset API Server to connect to. | https://api.rs2.usw2.rockset.com |
| `rockset.collection` | The name of the Rockset collection into which this connector will write. |  |
| `rockset.apikey` | API Key authenticates the connector to write into Rockset collections | |

#### General Optional Parameters

| Name | Description | Default Value |
|-------- |----------------------------|-----------------------|
| `rockset.task.threads` | Number of threads that each task should spawn when writing to Rockset. | 5 |

## License

Rockset Connect for Kafka is licensed under the Apache License 2.0.
