# Apache Kafka Metrics Exporter
The component can be used to extract Kafka component metrics in Jolokia format. It is a set of python scripts created to poll "all" available JMX metrics in Jolokia format from Kafka ecosystem components. No configuration needed apart from enabling Jolokia on the target servers. Most of the systems require long and lengthy parsing configurations to make sure that they can inject the JMX beans. It handles everything for you and creates a normalized JSON structure from the JMX data. 

The component is completely stateless and could be restarted as and when necessary. Being a python codebase, the footprint is pretty small and runs pretty light as it does not maintain any state. 

The ingress sources are:
* Apache Zookeeper
* Apache Kafka Core
* Apache Kafka Connect
* Confluent KSQL 
* Apache Kafka Connect connectors REST statistics.
* More to follow

The current egress sinks are:
* Apache Kafka Topic - This is the preferred mode of streaming the metrics as Kafka could scale with the load. Also, there are more than 100 connectors available int he Apache Kafka Connector library which would literally take me months to code on my own. ( This will become the de-facto storage layer for this component in the future for scalability purposes)
* ElasticSearch Index - The normalized messages are delivered into an index and name-value pairs created accordingly. The ElasticSearch component also has an addon module which creates the index as well. This module creates a set of basic dashboards for visualizing your Apache Kafka Cluster health.

How it works:
* Enable Jolokia ports for all necessary components. 
* Configure the necessary switches for the component. It will display help if you run the component without necessary switches.
* It will start streaming necessary data from the source targets to sink targets. 

Docker Repo : `docker pull abhiwalia/kafkajmxexporter:latest`
If you want to create your own container ( which will be the same as above ), you can use the `Dockerfile` in this codebase. 

Feel free to reach out to me in case you have some questions/comments. 
