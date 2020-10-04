import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor
from url_normalize import url_normalize

import ElasticSearchAppender
import JMXScraper
import KafkaAppender
import KubernetesAutomator

_executor = ThreadPoolExecutor(50)

# I will whole heartily recommend not resolving below 30 seconds as the process needs to execute all the URL's in a
# loop and the Java process will need time to breath , not some other process asking for metrics every 5-10 seconds.
POLL_WAIT_IN_SECS = 5
JMX_POLL_CONCURRENT_THREADS = 5

# # The list of endpoints to be farmed. The Structure is a Dictionary with the Server/Component type as the Key and
# # Value is a list of JMX URL's that need to be farmed for those servers.
url_list = {"ZooKeeper": ["http://localhost:49901/jolokia/read/org.apache.ZooKeeperService:*"],
            "KafkaBroker": ["http://localhost:49911/jolokia/read/kafka.*:*",
                            "http://localhost:49912/jolokia/read/kafka.*:*"],
            "KafkaConnect": ["http://localhost:49921/jolokia/read/kafka.*:*"],
            "KSQL": ["http://localhost:49931/jolokia/read/kafka.*:*"]
            }

# Accepted values form ingestion modules are one or more of the following
# "elastic", "kafka"
# Currently only elastic & kafka work - others are being worked on.
ingestion_modules = ["elastic", "kafka"]

# This switch will enable scrape for Connect REST modules and add a new
# JMX metric line for ingestion to all the sources
enable_connect_rest_scrape = True

enable_k8s = False


def finalize_url_list(args_list):
    k8s_pods = dict()
    global enable_k8s

    if enable_k8s:
        k8s_pods = KubernetesAutomator.get_pod_details()

    inner_url_list = dict()
    current_filter = "ZooKeeper"
    if args_list.zk_server_list or k8s_pods.get(current_filter, False):
        inner_url_list[current_filter] = return_url_set((args_list.zk_server_list if args_list.zk_server_list else []) + k8s_pods.get(current_filter, []),
                                                        args_list.zk_mbeans_list)
    current_filter = "KafkaBroker"
    if args_list.kafka_server_list or k8s_pods.get(current_filter, False):
        inner_url_list[current_filter] = return_url_set((args_list.kafka_server_list if args_list.kafka_server_list else []) + k8s_pods.get(current_filter, []),
                                                        args_list.kafka_mbeans_list)
    current_filter = "KafkaConnect"
    if args_list.connect_server_list or k8s_pods.get(current_filter, False):
        inner_url_list[current_filter] = return_url_set((args_list.connect_server_list if args_list.connect_server_list else []) + k8s_pods.get(current_filter, []),
                                                        args_list.connect_mbeans_list)
    current_filter = "KSQL"
    if args_list.ksql_server_list or k8s_pods.get(current_filter, False):
        inner_url_list[current_filter] = return_url_set((args_list.ksql_server_list if args_list.ksql_server_list else []) + k8s_pods.get(current_filter, []),
                                                        args_list.ksql_mbeans_list)

    if enable_k8s:
        for k, v in k8s_pods.items():
            if k not in ["ZooKeeper", "KafkaBroker", "KafkaConnect", "KSQL"]:
                inner_url_list[k] = return_url_set(
                    v, args_list.common_mbeans_list)
    return inner_url_list


async def main_loop(calling_object_method, jmx_data_node):
    task_list = []
    for call_type in calling_object_method:
        for data_node in jmx_data_node.values():
            if "elastic" in call_type:
                task_list.append(loop.run_in_executor(_executor,
                                                      ElasticSearchAppender.call_elastic_bulk, data_node))
            if "kafka" in call_type:
                task_list.append(loop.run_in_executor(_executor, KafkaAppender.produce_messages_to_kafka,
                                                      data_node, JMXScraper.last_fetch_timestamp, KafkaAppender.DEFAULT_KAFKA_PRODUCER))
    await asyncio.gather(*task_list)


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Command line arguments for controlling the application", add_help=True, )
    global_args = parser.add_argument_group("global", "Global Arguments")
    jmx_args = parser.add_argument_group(
        "jmx-poll", "JMX Poller module Arguments")
    es_args = parser.add_argument_group(
        "elastic", "Elastic Sink module Arguments")
    kafka_args = parser.add_argument_group(
        "kafka", "Kafka Sink module Arguments")
    connect_rest_args = parser.add_argument_group(
        "connect rest api", "Kafka Connect REST API module Arguments")

    global_args.add_argument('--enable-elastic-sink', action="store_true", default=None,
                             help="Enables the Elastic Sink for the JMX metrics. Needs configurations for Elastic Sink Module Arguments")
    global_args.add_argument('--enable-kafka-sink', action="store_true", default=None,
                             help="Enables the Elastic Sink for the JMX metrics. Needs configurations for Kafka Sink Module Arguments")
    global_args.add_argument('--enable-connect-rest-source', action="store_true", default=False,
                             help="Enables the Kafka Connect REST API metrics and publish them as part of the JMX Metrics. Needs configurations for Connect REST API Module Arguments")
    global_args.add_argument('--poll-interval', type=int, default=5, metavar=5,
                             help="Poll Interval to check if JMX metrics are refreshed in the memory or not")
    global_args.add_argument('--thread-count', type=int, default=20, metavar=20,
                             help="Thread pool to create for executing HTTP requests from the code. The HTTP requests include Elastic Bulk requests & Kafka Producer requests.")

    jmx_args.add_argument('--jmx-poll-thread-count', type=int, default=25, metavar=25,
                          help='Thread pool to fetch JMX metrics. This thread pool is independent from the HTTP call thread pool and is used to fetch the JMX metrics from the servers.')
    jmx_args.add_argument('--jmx-poll-wait-sec', type=int, default=60, metavar=20,
                          help='This is the poll duration which is enacted on JMX module only. The reason is that the poll for any new data from sink modules need to be decoupled from JMX fetch so that we do not overload the jolokia servers. The JMX module runs its own poll and refreshes the data following this particular value. This value cannot be assigned a value below 15 seconds due to overload switch.')
    jmx_args.add_argument('--jmx-poll-timeout', type=int, default=10, metavar=10,
                          help='This parameter will help override the timeout wait for JMX fetch via jolokia.')

    jmx_args.add_argument('--jmx-enable-k8s-discovery', action="store_true", default=None,
                          help="Enable this switch to allow the code to scan deployed components as Kubernetes pods and gather required Jolokia URL's")
    jmx_args.add_argument('--jmx-k8s-context', type=str, default=argparse.SUPPRESS, metavar="gke_projectname_region_cluster",
                          required='--jmx-enable-k8s-discovery' in sys.argv, help='The kube context used to determine which cluster to work with. The context should be present in the local kube_config and is auto injected in Kuberenetes Pods.')
    jmx_args.add_argument('--jmx-k8s-jolokia-enabled-annotation', type=str, default="jolokia/is_enabled", metavar="jolokia/is_enabled",
                          help='The annotation to scan K8s for Jolokia Scrape to be enabled')
    jmx_args.add_argument('--jmx-k8s-jolokia-server-type-annotation', type=str, default="argparse.SUPPRESS", metavar="jolokia/server_type",
                          help='The annotation to scan K8s for identifying the server type -- KafkaBroker, KafkaConnect etc. Defaulted to Discovery')
    jmx_args.add_argument('--jmx-k8s-jolokia-port-name-matcher', type=str, default="jolokia", metavar="jolokia",
                          help='The port name in K8s pod spec to be used for identifying jolokia port number.')
    jmx_args.add_argument('--jmx-k8s-label-filter', type=str, metavar="key1=value1, key2=value2", action="append", dest="k8s_label_filter_list",
                          help='Any additional label filters to be added to the K8s discovery. By default, no label filters are used.')
    jmx_args.add_argument('--jmx-k8s-field-filter', type=str, metavar="key1=value1, key2=value2", action="append", dest="k8s_field_filter_list",
                          help='Any additional field selector filters to be added to the K8s discovery. By default, status.phase=Running is always appended and cannot be removed')

    jmx_args.add_argument('--jmx-zk-server', type=str, metavar="http://localhost:49901/", action="append", dest="zk_server_list",
                          help='The zookeeper servers comma separated values in the format: http(s)://<hostname>:<port>.  The port number is the exposed Jolokia port for scraping the metrics.')
    jmx_args.add_argument('--jmx-kafka-server', type=str, metavar="http://localhost:49911/", action="append", dest="kafka_server_list",
                          help='The Apache Kafka servers comma separated values in the format: http(s)://<hostname>:<port>. The port number is the exposed Jolokia port for scraping the metrics.')
    jmx_args.add_argument('--jmx-connect-server', type=str, metavar="http://localhost:49921/", action="append", dest="connect_server_list",
                          help='The Apache Kafka Connect servers comma separated values in the format: http(s)://<hostname>:<port>. The port number is the exposed Jolokia port for scraping the metrics.')
    jmx_args.add_argument('--jmx-ksql-server', type=str, metavar="http://localhost:49931/", action="append", dest="ksql_server_list",
                          help='The Confluent KSQL servers comma separated values in the format: http(s)://<hostname>:<port>. The port number is the exposed Jolokia port for scraping the metrics.')

    jmx_args.add_argument('--jmx-zk-poll-mbean', type=str, metavar="org.apache.ZooKeeperService:*", default=["org.apache.ZooKeeperService:*", ], action="append", dest="zk_mbeans_list",
                          help='The MBeans that will be polled from the ZooKeeper server periodically. The beans follow the formatting conventions required by Jolokia and the service will fail in case the formatting is incorrect. Eg: "org.apache.ZooKeeperService".')
    jmx_args.add_argument('--jmx-kafka-poll-mbean', type=str, metavar="kafka.*:*", default=["kafka.*:*", ], action="append", dest="kafka_mbeans_list",
                          help='The MBeans that will be polled from the Kafka server periodically. The beans follow the formatting conventions required by Jolokia and the service will fail in case the formatting is incorrect. Eg: "kafka.*:*"')
    jmx_args.add_argument('--jmx-connect-poll-mbean', type=str, metavar="kafka.*:*", default=["kafka.*:*", ], action="append", dest="connect_mbeans_list",
                          help='The MBeans that will be polled from the Kafka connect server(s) periodically. The beans follow the formatting conventions required by Jolokia and the service will fail in case the formatting is incorrect. Eg: "kafka.*:*"')
    jmx_args.add_argument('--jmx-ksql-poll-mbean', type=str, metavar="kafka.*:*", default=["kafka.*:*", "io.confluent.*:*"], action="append", dest="ksql_mbeans_list",
                          help='The MBeans that will be polled from the Confluent KSQL server(s) periodically. The beans follow the formatting conventions required by Jolokia and the service will fail in case the formatting is incorrect. Eg: "kafka.*:*"')
    jmx_args.add_argument('--jmx-default-bean', type=str, metavar="java.lang:type=*", default=["java.lang:type=Memory", ], action="append", dest="common_mbeans_list",
                          help='The MBeans that will be polled from all the servers periodically. These are common pattern mbeans that you would want to poll from all the servers. The beans follow the formatting conventions required by Jolokia and the service will fail in case the formatting is incorrect. Eg: "java.lang:type=*"')

    connect_rest_args.add_argument('--connect-thread-count', type=int, default=5, metavar=5,
                                   help='Thread pool to fetch Connect REST metrics. This thread pool is independent from the HTTP call thread pool and is used to fetch the Connect REST metrics from the servers.')
    connect_rest_args.add_argument('--connect-rest-endpoint', type=str, required='--enable-connect-rest-source' in sys.argv, metavar="http://localhost:8083",
                                   help='Connect REST endpoint URL. This is strongly recommended to be the load balanced connect REST URL, so that atleast one of the servers is avaiable all the time.')
    connect_rest_args.add_argument('--enable-connect-rest-auth', action="store_true",
                                   help='Enable authentication for connect REST api poll. Please remember that currently only basic aut is supported.')
    connect_rest_args.add_argument('--connect-rest-auth-user', type=str, default=argparse.SUPPRESS, metavar="superUser",
                                   required='--enable-connect-rest-auth' in sys.argv, help='Connect basic auth username')
    connect_rest_args.add_argument('--connect-rest-auth-pass', type=str, default=argparse.SUPPRESS, metavar="superUser",
                                   required='--enable-connect-rest-auth' in sys.argv, help='Connect basic auth password')

    es_args.add_argument('--es-url', type=str, default=argparse.SUPPRESS, required='--enable-elastic-sink' in sys.argv, metavar="http://localhost:9021/",
                         help='Elastic Search URL for shipping the data to Elastic from this module. Load Balanced URL preferred.')
    es_args.add_argument('--kibana-url', type=str, default=argparse.SUPPRESS, required='--enable-elastic-sink' in sys.argv, metavar="http://localhost:5601/",
                         help='Kibana URL for creating the dashboards and indexes during the initial setup of the script. Load Balanced URL preferred.')
    es_args.add_argument('--es-bulk-url-timeout', type=int, default=30, metavar=30,
                         help='This parameter controls the timeout for bulk api insertion used by the module. Wont need to change for most cases, but just in case. :) ')

    kafka_args.add_argument('--kafka-topic-name', type=str, default="jmx_data_ingestion_pipeline", required='--enable-kafka-sink' in sys.argv,
                            help='Kafka Topic name for ingesting data from the JMX metrics into. Please remember that this module will produce one message per metric per poll per server. So provision enough partitions and data retention as per requirements.')
    kafka_args.add_argument('--kafka-conn-props', required='--enable-kafka-sink' in sys.argv, action="append", dest="kafka_connection",
                            help='One key value per prop switch separated by =. All of them will be added to the kafka producer connection.')

    args = parser.parse_args()
    # pprint.pprint(args)

    if not (args.enable_elastic_sink or args.enable_kafka_sink):
        parser.error(
            'No sink provided, add --enable-elastic-sink or --enable-kafka-sink')

    if not (args.zk_server_list or args.kafka_server_list or args.connect_server_list or args.ksql_server_list or args.jmx_enable_k8s_discovery):
        parser.error(
            'No JMX Scrape locations provided, add atleast one of --jmx-zk-server, --jmx-kafka-server, --jmx-ksql-server, --jmx-connect-server or --jmx-enable-k8s-discovery')

    connection_props = dict()
    if args.kafka_connection:
        for item in args.kafka_connection:
            k, v = item.split("=", 1)
            connection_props[k] = v

    import itertools

    def return_url_set(list1, list2):
        if None not in (list1, list2):
            return list(url_normalize(k[0] + "/jolokia/read/" + k[1])
                        for k in itertools.product(list1, list2))
        else:
            return None

    k8s_pods = dict()
    enable_k8s = False
    if args.jmx_enable_k8s_discovery:
        labels_dict = dict()
        fields_dict = dict()

        if args.k8s_label_filter_list:
            for item in args.k8s_label_filter_list:
                k, v = item.split("=", 1)
                labels_dict[k] = v
        if args.k8s_field_filter_list:
            for item in args.k8s_field_filter_list:
                k, v = item.split("=", 1)
                fields_dict[k] = v
        enable_k8s = KubernetesAutomator.setup_everything(kube_label_filter_dict=labels_dict,
                                                          kube_field_filter_dict=fields_dict,
                                                          kube_context=args.jmx_k8s_context)

    url_list = dict()
    default_JMX_URLs = return_url_set(["", ],
                                      args.common_mbeans_list)

    url_list = finalize_url_list(args)

    POLL_WAIT_IN_SECS = args.poll_interval
    ingestion_modules = []
    if args.enable_elastic_sink:
        ingestion_modules.append("elastic")
    if args.enable_kafka_sink:
        ingestion_modules.append("kafka")

    print(args)

    JMXScraper.setup_everything(url_list, default_JMX_URLs,
                                poll_wait=(15 if args.jmx_poll_wait_sec < 15
                                           else args.jmx_poll_wait_sec),
                                thread_count=args.jmx_poll_thread_count,
                                connect_rest_enabled=args.enable_connect_rest_source,
                                input_call_timeout_in_secs=args.jmx_poll_timeout)
    print(json.dumps(JMXScraper.url_list, indent=2))
    if args.enable_elastic_sink:
        ElasticSearchAppender.setup_elastic_connection(elasticsearch_endpoint=args.es_url,
                                                       elasticsearch_index_name="kafka-jmx-logs",
                                                       kibana_endpoint=url_normalize(
                                                           args.kibana_url),
                                                       es_bulk_url_timeout=args.es_bulk_url_timeout)
    if args.enable_kafka_sink:
        KafkaAppender.setup_kafka_connection(DEFAULT_TOPIC_NAME=args.kafka_topic_name,
                                             PRODUCER_CONFIGS=connection_props)
    if args.enable_connect_rest_source:
        import ConnectRESTMetrics
        ConnectRESTMetrics.setup_everything(CONNECT_REST_ENDPOINT=args.connect_rest_endpoint,
                                            CONCURRENT_THREADS=4,
                                            AUTH_ENABLED=args.enable_connect_rest_auth,
                                            AUTH_USERNAME=args.connect_rest_auth_user,
                                            AUTH_PASSWORD=args.connect_rest_auth_pass)
    runCode = True
    if runCode:
        loop = asyncio.get_event_loop()
        while (True):
            print("Metrics Gather poll session started at time \t" +
                  time.strftime("%Y-%m-%d %H:%M:%S"))
            if (JMXScraper.get_metrics(force_metric_collection=False)):
                print("New data updated in the JMX object. Please retrieve from there")
                start_time = time.perf_counter()
                loop.run_until_complete(
                    main_loop(ingestion_modules, JMXScraper.jmx_metrics_data))
                end_time = time.perf_counter() - start_time
                print(
                    f"Ingestion comnplete cycle finished in {end_time:0.2f} seconds.")
                if enable_k8s:
                    print("Refreshing pod list from K8s")
                    url_list = finalize_url_list(args)
                    JMXScraper.setup_everything(url_list, default_JMX_URLs,
                                                poll_wait=(15 if args.jmx_poll_wait_sec < 15
                                                           else args.jmx_poll_wait_sec),
                                                thread_count=args.jmx_poll_thread_count,
                                                connect_rest_enabled=args.enable_connect_rest_source,
                                                input_call_timeout_in_secs=args.jmx_poll_timeout)
            else:
                print("No new data received this cycle.Please try again later")
            print("=" * 120)
            time.sleep(POLL_WAIT_IN_SECS)
        loop.close()
