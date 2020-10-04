from kubernetes import client, config
import traceback

core_api = ""
label_selector = ""
field_selector = ""
is_inside_k8s = False
enabled_check_annotation = "jolokia/is_enabled"
server_type_annotation = "jolokia/server_type"
jolokia_port_annotation = "jolokia/port"
k8s_port_name = "jolokia"
pod_limit_per_request = 50
k8s_url_dict = {}


def selector_string(selector_dict: dict):
    return ",".join([str(k) + "=" + str(v)
                              for k, v in selector_dict.items()])


def setup_everything(kube_label_filter_dict: dict = {},
                     kube_field_filter_dict: dict = {},
                     kube_context="docker-desktop"):
    global label_selector
    global field_selector
    global enabled_check_annotation
    global server_type_annotation
    global jolokia_port_annotation
    global is_inside_k8s
    label_selector = selector_string(kube_label_filter_dict)
    field_selector = selector_string({**kube_field_filter_dict,
                                      "status.phase": "Running"})

    print("=" * 120)
    print("Current K8s Context (This is IMPORTANT): \t\t" + kube_context)
    print("Current label Filters: \t\t\t\t\t" + label_selector)
    print("Current field Filters: \t\t\t\t\t" + field_selector)
    print("Pods filter Annotation: \t\t\t\t" + enabled_check_annotation)
    print("Pods port Annotation: \t\t\t\t\t" + jolokia_port_annotation)
    print("Pod Server Type annotation (or default to Discovered): \t" +
          server_type_annotation)
    # print("Pod Jolokia Port annotation:\t\t\t\t" + jolokia_port_annotation)
    print("=" * 120)

    global core_api
    try:
        config.load_incluster_config()
        is_inside_k8s = True
        print(" In cluster configuration is detected. Please make sure that the Internal IP addresses are reachable from the pod for proper functioning. Otherwise the collector will silently fail.")
    except config.ConfigException:
        config.load_kube_config(context=kube_context)
        is_inside_k8s = False
        print(str.upper("Out of cluster configuration is detected. This mode is strictly provided to enable debugging local K8s cluster as pod IP addresses outside of Kubernetes are not generally reachable. If you are confident that the pod IP's are reachable, feel free to use this mode, otherwise deploy this container to the same Kubernetes cluster and it will pick up the right IP addresses automatically."))

    try:
        core_api = client.CoreV1Api()
    except client.rest.ApiException as error:
        print(error)
        print("Cannot Initialize the K8s Client API. Ignoring K8s inputs")
        return False
    return True


def add_server_to_fetch_list(poditems_list: list):
    for pod in poditems_list:
        if pod.metadata.annotations is not None and (enabled_check_annotation in pod.metadata.annotations.keys()):
            server_type_label = pod.metadata.annotations.get(
                server_type_annotation, "Discovered")
            if pod.metadata.annotations is not None and (jolokia_port_annotation in pod.metadata.annotations.keys()):
                current_list = k8s_url_dict.get(
                    server_type_label)
                jolokia_url = "http://" + \
                    str(pod.status.pod_ip) + ":" + \
                    str(pod.metadata.annotations[jolokia_port_annotation])
                if current_list is None:
                    current_list = [jolokia_url]
                else:
                    current_list.append(jolokia_url)
                k8s_url_dict.update({server_type_label: current_list})

            # if pod.spec.containers is not None:
            #     for container in pod.spec.containers:
            #         if container.ports is not None:
            #             current_list = k8s_url_dict.get(
            #                 server_type_label)
            #             for port in container.ports:
            #                 # print(port)
            #                 if str.upper(port.name) == str.upper(k8s_port_name) and str.upper(port.protocol) == "TCP":
            #                     jolokia_url = "http://" + \
            #                         str(pod.status.pod_ip) + ":" + \
            #                         str(port.container_port)
            #                     if current_list is None:
            #                         current_list = [jolokia_url]
            #                     else:
            #                         current_list.append(jolokia_url)


def get_pod_details(kwargs={}):
    global core_api
    global label_selector
    global field_selector
    global pod_limit_per_request
    global enabled_check_annotation
    global server_type_annotation
    global k8s_url_dict

    def get_pod_details_inner(kwargs):
        pod_details = core_api.list_pod_for_all_namespaces_with_http_info(watch=False,
                                                                          label_selector=label_selector,
                                                                          field_selector=field_selector,
                                                                          **kwargs)
        # for i in pod_details[0].items:
        #     print("%s\t%s\t%s\t%s" %
        #           (i.status.pod_ip, i.metadata.namespace, i.metadata.name, i.metadata.annotations if i.metadata.annotations is not None else ""))
        return pod_details, pod_details[0].metadata._continue

    addl_kwargs = {"limit": pod_limit_per_request,
                   **kwargs}
    pod_details, continue_flag = get_pod_details_inner(addl_kwargs)
    add_server_to_fetch_list(pod_details[0].items)
    while (continue_flag is not None):
        addl_kwargs.update({"_continue": continue_flag})
        pod_details, continue_flag = get_pod_details_inner(addl_kwargs)
        add_server_to_fetch_list(pod_details[0].items)
    return(k8s_url_dict)


if __name__ == "__main__":
    enable_k8s = setup_everything(
        kube_context="gke_solutionsarchitect-01_us-central1_abhi-test")
    if enable_k8s:
        resp = get_pod_details()
        print(k8s_url_dict)
