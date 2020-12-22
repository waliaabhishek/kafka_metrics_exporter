import base64
import io
import json
import time
from elasticsearch.helpers import bulk
from url_normalize import url_normalize

import requests
from requests.auth import HTTPBasicAuth
from elasticsearch import Elasticsearch

# the url for Elastic connection. No support for SSL yet. maybe some time soon ,
# but need to get the code running first.
es_url = ""
# The index will automatically be created with a date extensions to create index
# per day and keep the clean up easier
#  You will still have to create a read the timestamp in millis ( createdDateTime )
#  in elastic & extract a date from it
es_index_name = ""
# this is the Kibana dashboard URL. This is needed for the initial index, visualization
# and dashboard setup.
kibana_url = ""
kibana_dashboard_file_location = ""
es_bulk_url_timeout_secs = 30


def setup_elastic_connection(elasticsearch_endpoint="http://localhost:9200",
                             elasticsearch_index_name="kafka-jmx-logs",
                             kibana_endpoint="http://localhost:5601",
                             es_bulk_url_timeout=30,
                             kibana_dashboard_filename="scripts/dashboard/jmx_dashboard.ndjson"
                             ):
    global es_url
    global es_index_name
    global kibana_url
    global kibana_dashboard_file_location
    global es_bulk_url_timeout_secs
    es_url = url_normalize(elasticsearch_endpoint)
    es_index_name = elasticsearch_index_name
    kibana_url = url_normalize(kibana_endpoint)
    es_bulk_url_timeout_secs = es_bulk_url_timeout
    kibana_dashboard_file_location = kibana_dashboard_filename
    create_elastic_index_template()


def create_elastic_index_template():
    global es_index_name
    global es_url
    global kibana_url
    global kibana_dashboard_file_location
    # Create first set of headers for inserting the templates
    request_headers = {
        'Content-Type': 'application/json',
    }
    request_headers_form = {
        'kbn-xsrf': 'true',
    }
    # setup the body for inserting the templates
    data = '{"index_patterns": ["' + es_index_name + '-*"],"template":{ \
        "mappings": {"properties": {"createdDateTime": {"type": "date"}}}}}'
    # Insert the template into Elastic for datatime formatting
    requests.put(url_normalize(es_url + '/_index_template/' + es_index_name + '_template'), headers=request_headers,
                 data=data)
    # Setup the headers for inserting index
    request_headers['kbn-version'] = '7.10.1'
    request_headers['kbn-xsrf'] = 'true'
    # Create the index pattern for Kibana
    files = {'file': ('export.ndjson', open(kibana_dashboard_file_location, 'rb'))}
    requests.post(url_normalize(kibana_url + '/api/saved_objects/_import'), headers=request_headers_form,
                  auth=HTTPBasicAuth('user', 'pass'), files=files)


# Prepare data and inject to elastic index
def call_elastic_bulk(data_dict):
    global es_url
    actions = []
    es_client = Elasticsearch([es_url],
                              retry_on_timeout=True,
                              max_retries=10,
                              timeout=es_bulk_url_timeout_secs,
                              http_auth=('user', 'secret'))

    for item in data_dict:
        actions.append(str(item))
    curr_index_name = es_index_name + "-" + time.strftime("%Y-%m-%d")
    bulk(es_client, index=curr_index_name, actions=actions, params={
        "timeout": str(es_bulk_url_timeout_secs) + "s", "request_timeout": es_bulk_url_timeout_secs}, refresh=True,
         stats_only=True, raise_on_error=False)
