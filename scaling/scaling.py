"""Handle scaling """
import base64
import logging
from monitoring import dataproc_monitoring, metrics
from util import pubsub
from util import settings

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
    data = base64.b64decode(payload).split(",")
    # ScaleOutYARNMemoryAvailablePercentage or
    # ScaleInYARNMemoryAvailablePercentagedata[0]
    # ScaleOutContainerPendingRatio data[1]
    logging.info(
        "YARNMemoryAvailablePercentage {} ContainerPendingRatio{}".format(
            data[0], data[1], data[2]))
    met = metrics.Metrics()
    met.write_timeseries_value('YARNMemoryAvailablePercentage', data[0])
    met.write_timeseries_value('ContainerPendingRatio', data[1])
    met.write_timeseries_value('YarnNodes', data[2])
    if int(data[1]) == -1 or int(data[0]) == -1:
        if int(data[2]) > settings.get_key('MinInstances'):
            trigger_scaling("down")
        return 'OK', 204
    if int(data[0]) < settings.get_key(
            'ScaleOutYARNMemoryAvailablePercentage'):
        trigger_scaling("up")
    elif float(data[1]) > settings.get_key('ScaleOutContainerPendingRatio'):
        trigger_scaling("up")
    elif int(data[0]) > settings.get_key(
            'ScaleInYARNMemoryAvailablePercentage'):
        trigger_scaling("down")

    return 'OK', 204


def calc_scale(data):
    """
    How many nodes to add
    :param data:
    :return:
    """
    scaling_by = 1
    if data == 'down':
        scaling_by = -1
    return scaling_by


def do_scale(payload):
    """
    Perform the scaling
    :param payload:
    :return:
    """
    data = base64.b64decode(payload)
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
    scaling_by = calc_scale(data)
    new_size = current_nodes + scaling_by
    if new_size > settings.get_key('MaxInstances'):
        new_size = settings.get_key('MaxInstances')
    if new_size < settings.get_key('MinInstances'):
        new_size = settings.get_key('MinInstances')
    if new_size == current_nodes:
        return 'kNot Modified', 304
    logging.info(
        "Updating cluster from {} to {} nodes".format(current_nodes, new_size))
    try:
        dp.patch_cluster(new_size)
    except dataproc_monitoring.DataProcException as e:
        logging.error(e)
        return 'Error', 500
    return 'ok', 204
