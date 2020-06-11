# Apache Kafka JMX Metrics Extractor and Dashboard
Extract JMX Metrics in Jolokia format, ingest into Elastic and setup a dashboard to view it.

The setup only works with metrics from Zookeepeer , Kafka Brokers & Kafka Connect Services at the moment.
Will be adding more support in the future. 

How it works:
* Enable JMX ports for all ZK , Kafka & Connect servers. 
* Open Parse_JMX.py and edit the `urllist` attribute.
* Update the ElasticSearch Server URL (`elasticURL`) and Kibana URL (`kibanaURL`) as well.
* Run the Docker Compose after running Confluent platform.
