# Apache Kafka Metrics Exporter

The component can be used to extract Kafka component metrics in Jolokia format. It is a set of python scripts created to poll "all" available JMX metrics in Jolokia format from Kafka ecosystem components. No configuration needed apart from enabling Jolokia on the target servers. Most of the systems require long and lengthy parsing configurations to make sure that they can inject the JMX beans. It handles everything for you and creates a normalized JSON structure from the JMX data.

The component is completely stateless and could be restarted as and when necessary. Being a python codebase, the footprint is pretty small and runs pretty light as it does not maintain any state.

## The ingress sources are:

- [Apache Zookeeper](#Apache-Zookeeper)
- [Apache Kafka Core](#Apache-Kafka)
- [Apache Kafka Connect](#Apache-Kafka-Connect)
- [Confluent KSQL](#Confluent-KSQL)
- [Apache Kafka Connect connectors REST statistics](#Apache-Kafka-Connect-REST-API)
- [Kubernetes Auto Scraping](#Apache-Kafka-deployed-on-Kubernetes)

## The current egress sinks are:

- [Apache Kafka](#Apache-Kafka-Sink)
- [ElasticSearch](#Elasticsearch-Sink)

## How it works:

- Enable Jolokia ports for all necessary components.
- Configure the necessary switches for the component. It will display help if you run the component without necessary switches.
- It will start streaming necessary data from the source targets to sink targets.

Docker Repo : `docker pull abhiwalia/kafkajmxexporter:latest`
If you want to create your own container ( which will be the same as above ), you can use the `Dockerfile` in this codebase.

Feel free to reach out to me in case you have some questions/comments.

## Examples

The program is completely argument based and all argument help is provided as part of the program.

### Prerequistite steps

The code contains a requirements.txt file for all dependency installs.
Please make sure that you run that before executing the code.

```
git clone https://github.com/waliaabhishek/kafka_metrics_exporter
cd kafka_metrics_exporter/Code
pip install requirements.txt
```

I will be considering that you are in the `kafka_metrics_exporter/Code` for the rest of the examples.
Please confirmt hat before proceeding.

### List all the Argument options and their related help:

```
python main.py --help
```

Another thing to note here is that if you run the program without supplying any arguments, it will error out due to validations performed in the code to check for certain required paramaters.
The following are the options:

- **Alteast 1 of the available 4 Source Targets are mandatory**. The available options are --jmx-zk-server, --jmx-kafka-server, --jmx-enable-k8s-discovery or --jmx-connect-server
- **Atleast 1 Sink Target is mandatory.** The available options are --enable-elastic-sink or --enable-kafka-sink.
  We will dig a bit into each in a detailed table below.

### Ingress Details

### Apache Kafka deployed on premise (non K8s install/non ephemeral install)

This option considers that you are not running the dockerized version of the Confluent Platform and/or Apache Kafka.
The Confluent Platform (non dockerized) will most likely be deployed to VMs or bare metal machines which have static IP address and DNS names.
The original code was written to cater to this approach and has the following arguments to support it.

#### Apache Zookeeper

To enable ZooKeeper Polling add the following arguments:

```
--jmx-zk-server http://<zk1>:<Jolokia_port> --jmx-zk-server http://<zk2>:<Jolokia_port> --jmx-zk-server http://<zk3>:<Jolokia_port>
```

- Additional Switches

`--jmx-zk-poll-mbean` --> This is defaulted to `org.apache.ZooKeeperService:*`, which pulls every JMX MBean for ZooKeeper automatically.
If you do not want to poll all that, you can narrow it down to what specific MBean that you want. EG;

```
--jmx-zk-poll-mbean <MBEAN1> --jmx-zk-poll-mbean <MBEAN2>
```

**Mentioning the http is important as the code defaults to https as the primary protocol if it is not added.**

```
--jmx-zk-server localhost:7777  is equivalent to  --jmx-zk-server https://localhost:7777

Instead, use:

--jmx-zk-server http://localhost:7777  is equivalent to  --jmx-zk-server http://localhost:7777
```

This applies for all the services below as well.

#### Apache Kafka

To enable Apache Kafka polling, add the following arguments:

```
--jmx-kafka-server http://<kafka1>:<Jolokia_port> --jmx-kafka-server http://<kafka2>:<Jolokia_port> --jmx-kafka-server http://<kafka3>:<Jolokia_port>

```

- Additional Switches

`--jmx-kafka-poll-mbean` --> This is defaulted to `kafka.*:*`, which pulls every JMX MBean for Apache Kafka automatically.
If you do not want to poll all that, you can narrow it down to what specific MBean that you want. Example:

```
--jmx-kafka-poll-mbean <MBEAN1> --jmx-kafka-poll-mbean <MBEAN2>
```

#### Apache Kafka Connect

To enable Apache Kafka Connect polling, add the following arguments:

```
--jmx-connect-server http://<connect1>:<Jolokia_port> --jmx-connect-server http://<connect2>:<Jolokia_port>

```

- Additional Switches

`--jmx-connect-poll-mbean` --> This is defaulted to `kafka.*:*`, which pulls every JMX MBean for Apache Kafka Connect automatically.
If you do not want to poll all that, you can narrow it down to what specific MBean that you want. Example:

```
--jmx-connect-poll-mbean <MBEAN1> --jmx-connect-poll-mbean <MBEAN2>
```

#### Apache Kafka Connect REST API

The program can also poll the Apache Kafka Connect REST API.
To leverage that functionality, the floowing switches are necessary:

- `--enable-connect-rest-source` : Mandatory : Effectively enables the Apache Kafka Connect REST API poll.
- `--connect-rest-endpoint` : Mandatory : Connect REST endpoint URL. This is strongly recommended to be the load balanced connect REST URL, so that atleast one of the servers is avaiable all the time.
- `--enable-connect-rest-auth` : Optional : Enable authentication for connect REST api poll. Please remember that currently only basic authentication is supported at the moment.
- `--connect-rest-auth-user` : mandatory if Auth is enabled : Connect basic auth username
- `--connect-rest-auth-pass` : mandatory if Auth is enabled : Connect basic auth password

```
--enable-connect-rest-source --connect-rest-endpoint https//localhost:8089 --enable-connect-rest-auth --connect-rest-auth-user <UserName> --connect-rest-auth-pass <Password>
```

#### Confluent KSQL

To enable Confluent KSQL polling, add the following arguments:

```
--jmx-ksql-server http://<ksql1>:<Jolokia_port> --jmx-ksql-server http://<ksql2>:<Jolokia_port>

```

- Additional Switches

`--jmx-ksql-poll-mbean` --> This is defaulted to `kafka.*:*, io.confluent.*:*`, which pulls every JMX MBean for Confluent KSQL automatically.
If you do not want to poll all that, you can narrow it down to what specific MBean that you want. Example:

```
--jmx-ksql-poll-mbean <MBEAN1> --jmx-ksql-poll-mbean <MBEAN2>
```

#### Default MBeans

Apart from specifying the MBeans particular to every component, you can add additional Global MBeans here.
These global MBeans will be added to Jolokia poll list for every component requisited for polling.

The Deafult MBean added for poll to all the components is `java.lang:type=*`.
This makes sure taht we are able to poll the Java.Lan engine related MBeans from all the components, apart from their targeted JMX beans.

You can override this piece as well using the below argument:

```
--jmx-default-bean MBean1 --jmx-default-bean MBean2
```

### Apache Kafka deployed on Kubernetes

This is where the game changes a bit.
Kubernetes being very API centric and all the data being available upfront allows you to poll the Kubernetes API to get all the addresses from the system and run the polling.
The core code relies on Kubernetes annotation system and **needs permission to list all the pods for all namespaces**.

Please make sure you have the permission , otherwise server auto detection will never work.
The code will spit out the error in the log file and silently ignore Kubernetes input in that case.

Arguments and details:

- `--jmx-enable-k8s-discovery` : Mandatory : Enable this switch to allow the code to scan deployed components as Kubernetes pods and gather required Jolokia URL's
- `--jmx-k8s-context` : Mandatory if K8s discovery enabled : The kube context used to determine which cluster to work with. The context should be present in the local kube_config and/or is auto injected in Kuberenetes deployed pods.
- `--jmx-k8s-jolokia-enabled-annotation` : Optional :
  This annotation should be added to all the pods that you want scraped from the program.
  Only if this annotation is present, will the pods become eligible for scraping.
  The name for this annotation defaults to `jolokia/is_enabled`. The value for the annotation does not matter ( any arbitrary value could be used )
  If you want to change it , feel free to update the argument and the program will use the updated annotation name for scrape decision.
- `--jmx-k8s-jolokia-server-type-annotation` : Optional :
  This annotation is an optional server type identifier.
  If the annotation is not available, the code will mark the Server type as `Discovered`.
  If you see data having only `Discovered` as server type, this means that the annotation was not available but the scrape was enabled via the value of `--jmx-k8s-jolokia-enabled-annotation`.
  The server type are string values ( no special character ) like `ZooKeeper`, `ApacheKafka`, `KafkaConnect` , `KSQL`, `KafkaClient` etc.
  The name for this annotation defaults to `jolokia/server_type`.
  If you want to change it , feel free to update the argument and the program will use the updated annotation name for scrape decision.
  Please note that this will only update the lookup filter for the annotation.
  The name/value pair needs to be updated for each scrapable pod for it to function properly.

Usage:

The below case lets the other values to be set to default values.

```
--jmx-enable-k8s-discovery --jmx-k8s-context <k8s_context>
```

### Egress Details

There are 2 Egress options available.

- Apache Kafka Topic : Preferred option.
  This streams all the data collected from various sources as mentioned above into a single Kafka topic in a structured JSON format.
  The format is a flattened structure, so can be streamed to various systems like Splunk, Elasticsearch, New Relic, Prometheus , InfluxDB etc usign their resposctive Kafka Connectors.
  This is the preferred mode of streaming the metrics as Kafka could scale with the load.
  Also, there are more than 100 connectors available in the Apache Kafka Connector library which would literally take me years to code on my own. (This will become the de-facto storage layer for this component in the future for scalability purposes)
- Elasticsearch : The normalized messages are delivered into an index and name-value pairs created accordingly.
  The ElasticSearch component also has an addon module which creates the index as well.
  This module creates a set of basic dashboards for visualizing your Apache Kafka Cluster health.

### Apache Kafka Sink

The sink is pretty simple and just needs 2 arguments:

- `--enable-kafka-sink` : Optional : This switch enables the kafka sink module to stream the normalized data from within the program to a kafka topic.
- `--kafka-topic-name` : Mandatory if kafka sink is enabled : This is the kafka topic that you want to stream all the normalized data to be streamed to.
- `--kafka-conn-props` : Mandatory if kafka sink is enabled : These are the connection properties that you will need to provide for creating a kafka connection.

**The program does not support Schema Registry at the moment.**

Usage:

```
--enable-kafka-sink --kafka-topic-name abhishek_jmx_metrics --kafka-conn-props bootstrap.servers=localhost:12093 --kafka-conn-props client.id=JMX_Data_Producer --kafka-conn-props retries=10 --kafka-conn-props linger.ms=50
```

### Elasticsearch Sink:

The Elasticsearch sink streams the data directly to the ElasticSearch index by the name `kafka-jmx-logs` and cannot be changed at the moment as index name is part of the dashboard JSON file unfortunately.
The program creates a daily index with the date as part of the index name so that cleanup is easy for you.
As mentioned earlier, it needs access to Kibana URL as well to inject the dashboards directly into it, so that you dont have to do that on your own.

- `--enable-elastic-sink` : Optional : Enables the program module to activate the ElasticSearch sink streaming.
- `--es-url` : Madatory if elastic sink is enabled : Elastic Search URL for shipping the data to Elastic from this module. Load Balanced URL preferred.
- `--kibana-url` : Madatory if elastic sink is enabled : Kibana URL for creating the dashboards and indexes during the initial setup of the script. Load Balanced URL preferred.
- `--es-bulk-url-timeout` : Optional : This allows you to change your ElasticSearch ingestion timeout in case your ES cluster is slow due to any reason and the code keeps timing out for ingestion errors.

Usage:

```
--enable-elastic-sink --es-url http://localhost:9021/ --kibana-url http://localhost:5601/ --es-bulk-url-timeout 60
```
