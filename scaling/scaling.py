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

    # ScaleOutYARNMemoryAvailablePercentage or
    # ScaleInYARNMemoryAvailablePercentagedata[0]
    # ScaleOutContainerPendingRatio data[1]
    logging.info(
        "YARNMemoryAvailablePercentage {} ContainerPendingRatio{}".format(
            yarn_memory_available_percentage, container_pending_ratio,
            number_of_nodes))
    met = metrics.Metrics()
    met.write_timeseries_value('YARNMemoryAvailablePercentage',
                               yarn_memory_available_percentage)
    met.write_timeseries_value('ContainerPendingRatio',
                               container_pending_ratio)
    met.write_timeseries_value('YarnNodes', number_of_nodes)
    if container_pending_ratio == -1 or yarn_memory_available_percentage == -1:
        if number_of_nodes > settings.get_key('MinInstances'):
            trigger_scaling("down")
        return 'OK', 204
    if yarn_memory_available_percentage < settings.get_key(
            'ScaleOutYARNMemoryAvailablePercentage'):
        trigger_scaling("up")
    elif container_pending_ratio > settings.get_key(
            'ScaleOutContainerPendingRatio'):
        trigger_scaling("up")
    elif yarn_memory_available_percentage > settings.get_key(
            'ScaleInYARNMemoryAvailablePercentage'):
        trigger_scaling("down")

    return 'OK', 204


def calc_slope(minuets):
    """
    calculate the slope of available memory change
    :param minuets:
    """

    met = metrics.Metrics()
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


def calc_scale(current_nodes):
    """
    How many nodes to add
    :param current_nodes:
    :return:
    """
    scaling_by = current_nodes + (1 / calc_slope(60))
    logging.info("Scaling to {}".format(scaling_by))
    return int(scaling_by)


def do_scale():
    """
    Perform the scaling
    :return:
    """
    dp = dataproc_monitoring.DataProc()
    try:
        cluster_status = dp.get_cluster_status()
        current_nodes = int(dp.get_number_of_nodes())
    except dataproc_monitoring.DataProcException as e:
        logging.error(e)
        return 'Error', 500
    if cluster_status.lower() != 'running':
        logging.info("Cluster not ready for update status {}".format(
            cluster_status))
        return 'Not Modified', 304
    scaling_by = calc_scale(current_nodes)
    new_size = current_nodes + scaling_by
    if new_size > settings.get_key('MaxInstances'):
        new_size = settings.get_key('MaxInstances')
    if new_size < settings.get_key('MinInstances'):
        new_size = settings.get_key('MinInstances')
    if new_size == current_nodes:
        return 'Not Modified', 304
    logging.info(
        "Updating cluster from {} to {} nodes".format(current_nodes, new_size))
    try:
        dp.patch_cluster(new_size)
    except dataproc_monitoring.DataProcException as e:
        logging.error(e)
        return 'Error', 500
    return 'ok', 204
