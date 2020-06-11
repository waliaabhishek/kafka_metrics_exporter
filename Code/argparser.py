import argparse
import sys


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def append2dict(append_based_list):
    connection_props = dict()
    for item in append_based_list:
        k, v = item.split("=", 1)
        connection_props[k] = v
    return connection_props


def check_args(args=None):
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

    jmx_args.add_argument('--jmx-poll-thread-count', type=int, default=5, metavar=5,
                          help='Thread pool to fetch JMX metrics. This thread pool is independent from the HTTP call thread pool and is used to fetch the JMX metrics from the servers.')
    jmx_args.add_argument('--jmx-poll-wait-sec', type=int, default=20, metavar=20,
                          help='This is the poll duration which is enacted on JMX module only. The reason is that the poll for any new data from sink modules need to be decoupled from JMX fetch so that we do not overload the jolokia servers. The JMX module runs its own poll and refreshes the data following this particular value. This value cannot be assigned a value below 15 seconds due to overload switch.')
    jmx_args.add_argument('--jmx-poll-timeout', type=int, default=45, metavar=45,
                          help='This parameter will help override the timeout wait for JMX fetch via jolokia.')

    jmx_args.add_argument('--jmx-zk-server', type=str, metavar="http://localhost:49901/", action="append", dest="zk_server_list",
                          help='The zookeeper servers comma separated values in the format: http(s)://<hostname>:<port>.  The port number is the exposed Jolokia port for scraping the metrics.')
    jmx_args.add_argument('--jmx-kafka-server', type=str, metavar="http://localhost:49911/", action="append", dest="kafka_server_list",
                          help='The Apache Kafka servers comma separated values in the format: http(s)://<hostname>:<port>. The port number is the exposed Jolokia port for scraping the metrics.')
    jmx_args.add_argument('--jmx-connect-server', type=str, metavar="http://localhost:49921", action="append", dest="connect_server_list",
                          help='The Apache Kafka Connect servers comma separated values in the format: http(s)://<hostname>:<port>. The port number is the exposed Jolokia port for scraping the metrics.')

    jmx_args.add_argument('--jmx-zk-poll-mbean', type=str, metavar="org.apache.ZooKeeperService:*", default=["org.apache.ZooKeeperService:*", ], action="append", dest="zk_mbeans_list",
                          help='The MBeans that will be polled from the ZooKeeper server periodicatlly. The beans follow the formatting conventions required by Jolokia and the service will fail in case the formatting is incorrect. Eg: "org.apache.ZooKeeperService".')
    jmx_args.add_argument('--jmx-kafka-poll-mbean', type=str, metavar="kafka.*:*", default=["kafka.*:*", ], action="append", dest="kafka_mbeans_list",
                          help='The MBeans that will be polled from the Kafka server periodically. The beans follow the formatting conventions required by Jolokia and the service will fail in case the formatting is incorrect. Eg: "kafka.*:*"')
    jmx_args.add_argument('--jmx-connect-poll-mbean', type=str, metavar="kafka.*:*", default=["kafka.*:*", ], action="append", dest="connect_mbeans_list",
                          help='The MBeans that will be polled from the ZooKeeper server periodicatlly. The beans follow the formatting conventions required by Jolokia and the service will fail in case the formatting is incorrect. Eg: "kafka.*:*"')
    jmx_args.add_argument('--jmx-default-bean', type=str, metavar="java.lang:type=*", default=["java.lang:type=*", ], action="append", dest="common_mbeans_list",
                          help='The MBeans that will be polled from all the servers periodicatlly. These are common pattern mbeans that you would want to poll from all the servers. The beans follow the formatting conventions required by Jolokia and the service will fail in case the formatting is incorrect. Eg: "java.lang:type=*"')

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

    if not (args.zk_server_list or args.kafka_server_list or args.connect_server_list):
        parser.error(
            'No JMX Scrape locations provided, add --jmx-zk-server, --jmx-kafka-server or --jmx-connect-server')

    # if ((args.enable_connect_rest_source is True) and (args.connect_rest_endpoint is None)):
    #     parser.error(
    #         'If connect rest source is enabled, connect REST endpoint is required, add correct values for --enable-connect-rest-source and --connect-rest-endpoint')

    connection_props = dict()
    if args.kafka_connection:
        for item in args.kafka_connection:
            k, v = item.split("=", 1)
            connection_props[k] = v

    import itertools

    def return_url_set(list1, list2):
        if None not in (list1, list2):
            return list(k[0] + "/jolokia/read/" + k[1]
                        for k in itertools.product(list1, list2))
        else:
            return None

    url_list = dict()
    url_list["ZooKeeper"] = return_url_set(args.zk_server_list,
                                           args.zk_mbeans_list)
    url_list["KafkaBroker"] = return_url_set(args.kafka_server_list,
                                             args.kafka_mbeans_list)
    url_list["KafkaConnect"] = return_url_set(args.connect_server_list,
                                              args.connect_mbeans_list)
    default_JMX_URLs = return_url_set(list("/jolokia/read/"),
                                      args.common_mbeans_list)

    POLL_WAIT_IN_SECS = args.poll_interval
    ingestion_modules = []
    if args.enable_elastic_sink:
        ingestion_modules.append("elastic")
    if args.enable_kafka_sink:
        ingestion_modules.append("kafka")

    return args


if __name__ == "__main__":
    # arguments = check_args()
    # pprint.pprint(arguments)
    # import itertools
    # k = itertools.product(["z", "k", "C"], ["u1", "u2"])
    # # print(k[0] + k[1])
    # print(type(k))
    urlvalue = ["http://localhost:49911//jolokia/////read/kafka.*:*",
                "http://localhost:49912/jolokia/read///java.lang:type=*",
                "http://localhost:49911/jolokia/read/java.lang:type=*",
                "http://localhost:49912//jolokia/read/kafka.*:*"]
    print("=" * 120)
    from url_normalize import url_normalize
    for item in urlvalue:
        print(url_normalize(item))
