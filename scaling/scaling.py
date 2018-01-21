"""Handle scaling """
import base64
import json
import logging

import numpy as np

from monitoring import dataproc_monitoring, metrics
from util import pubsub, settings

SCALING_TOPIC = 'shamash-scaling'


def trigger_scaling(direction):
    """
    Start scaling operation
    :param direction:
    """
    logging.info("Trigger Scaling {}".format(direction))
    msg = {'messages': [{'data': base64.b64encode(direction)}]}
    pubsub_client = pubsub.get_pubsub_client()
    try:
        pubsub.publish(pubsub_client, msg, SCALING_TOPIC)
    except pubsub.PubSubException as e:
        logging.error(e)


def should_scale(payload):
    """
    Make a descion to scale or not
    :param payload:
    :return:
    """
    data = json.loads(base64.b64decode(payload))
    yarn_memory_available_percentage = data['yarn_memory_available_percentage']
    container_pending_ratio = data['container_pending_ratio']
    number_of_nodes = data['number_of_nodes']
    cluster_name = data['cluster']
    cluster_settings = settings.get_cluster_settings(cluster_name)
    # ScaleOutYARNMemoryAvailablePercentage or
    # ScaleInYARNMemoryAvailablePercentagedata[0]
    # ScaleOutContainerPendingRatio data[1]
    logging.info(
        "Cluster {} YARNMemoryAvailablePercentage {} ContainerPendingRatio{}".format(
            cluster_name,
            yarn_memory_available_percentage, container_pending_ratio,
            number_of_nodes))
    met = metrics.Metrics(cluster_name)
    met.write_timeseries_value('YARNMemoryAvailablePercentage',
                               yarn_memory_available_percentage)
    met.write_timeseries_value('ContainerPendingRatio',
                               container_pending_ratio)
    met.write_timeseries_value('YarnNodes', number_of_nodes)

    scaling = None
    if container_pending_ratio == -1 or yarn_memory_available_percentage == -1:
        if number_of_nodes > cluster_settings.MinInstances:
            scaling = "down"
    elif yarn_memory_available_percentage < cluster_settings.UpYARNMemAvailPct:
        scaling = "up"
    elif container_pending_ratio > cluster_settings.UpContainerPendingRatio:
        scaling = "up"
    elif yarn_memory_available_percentage > cluster_settings.DownYARNMemAvailePct:
        scaling = "down"
    body = {
        "cluster": cluster_name,
        "scaling": scaling
    }
    if scaling is not None:
        trigger_scaling(body)
    return 'OK', 204


def calc_slope(minuets, cluster):
    """
    calculate the slope of available memory change
    :param minuets:
    """

    met = metrics.Metrics(cluster)
    series = met.read_timeseries('YARNMemoryAvailablePercentage', minuets)
    retlist = []
    x = []
    y = []
    retlist.extend(series[0]['points'])
    i = len(retlist)
    for rl in retlist:
        x.insert(0, rl['value']['doubleValue'])
        y.insert(0, i)
        i = i - 1
    slope, intercept = np.polyfit(x, y, 1)
    return slope


def calc_scale(current_worker_nodes, current_preemptible_nodes,
               preemptiblepct, cluster_name):
    """
    How many nodes to add
    :param current_worker_nodes, current_preemptible_nodes,
               preemptiblepct, cluster_name:
    :return:
    """
    new_workers = current_worker_nodes + (1 / calc_slope(60, cluster_name)) * (
            1 - preemptiblepct) / 100
    new_preemptibe = current_preemptible_nodes + (
            1 / calc_slope(60, cluster_name)) * (
                             preemptiblepct / 100)
    logging.info("Scaling to {}".format(new_workers, new_preemptibe))
    return int(new_workers), int(new_preemptibe)


def do_scale(payload):
    """
    Perform the scaling
    :return:
    """
    data = json.loads(base64.b64decode(payload))
    dp = dataproc_monitoring.DataProc(data['cluster'])
    cluster_settings = settings.get_cluster_settings(data['cluster'])
    preemptiblepct = cluster_settings.PreemptiblePct
    try:
        cluster_status = dp.get_cluster_status()
        current_worker_nodes = int(dp.get_number_of_workers())
        current_preemptible_nodes = int(dp.get_number_of_preemptible_workers())
        current_nodes = int(dp.get_number_of_nodes())
    except dataproc_monitoring.DataProcException as e:
        logging.error(e)
        return 'Error', 500
    if cluster_status.lower() != 'running':
        logging.info("Cluster not ready for update status {}".format(
            cluster_status))
        return 'Not Modified', 200
    new_workers, new_preemptibel = calc_scale(current_worker_nodes,
                                              current_preemptible_nodes,
                                              preemptiblepct, data['cluster'])
    if new_workers < cluster_settings.MinInstances:
        new_workers = cluster_settings.MinInstances
    if (new_preemptibel + new_workers) > cluster_settings.MaxInstances:
        diff = (new_preemptibel + new_workers) - cluster_settings.MaxInstances
        new_preemptibel = new_preemptibel - int(
            diff * (preemptiblepct / 100)) - 1
        new_workers = new_workers - int(diff * (1 - preemptiblepct / 100))
        if new_workers < cluster_settings.MinInstances:
            new_workers = cluster_settings.MinInstances

    if (new_preemptibel + new_workers) == current_nodes:
        return 'Not Modified', 200
    logging.info(
        "Updating cluster from {} to {} nodes".format(current_nodes,
                                                      new_preemptibel + new_workers))
    try:
        dp.patch_cluster(new_workers, new_preemptibel)
    except dataproc_monitoring.DataProcException as e:
        logging.error(e)
        return 'Error', 500
    return 'ok', 204
