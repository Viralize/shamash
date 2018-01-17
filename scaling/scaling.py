import base64
import logging

from monitoring import dataproc_monitoring
from util import pubsub
from util import settings

SCALING_TOPIC = 'forseti-scaling'


def trigger_scaling(up):
    msg = {'messages': [{'data': base64.b64encode(up)}]}
    pubsub_client = pubsub.get_pubsub_client()
    pubsub.publish(pubsub_client, msg, SCALING_TOPIC)


def should_scale(payload):
    data = base64.b64decode(payload).split(",")
    # ScaleOutYARNMemoryAvailablePercentage or
    # ScaleInYARNMemoryAvailablePercentagedata[0]
    # ScaleOutContainerPendingRatio data[1]
    if int(data[0]) > settings.get_key('ScaleOutYARNMemoryAvailablePercentage'):
        trigger_scaling("up")
    elif float(data[1]) > settings.get_key('ScaleOutContainerPendingRatio'):
        trigger_scaling("up")
    elif int(data[0]) < settings.get_key('ScaleInYARNMemoryAvailablePercentage'):
        trigger_scaling("down")
    return 'OK', 200


def do_scale(payload):
    data = base64.b64decode(payload)
    dp = dataproc_monitoring.DataProc()
    cluster_data = dp.get_cluster_data()
    cluster_status = cluster_data['status']['state']
    if cluster_status.lower() != 'running':
        logging.info("Cluster not ready for update status {}".format(
            cluster_status))
        return
    current_nodes = int(
        cluster_data["metrics"]["yarnMetrics"][
            "yarn-vcores-available"])
    scaling_by = 1
    if data == 'down':
        scaling_by = -1
    new_size = current_nodes + scaling_by
    if new_size > settings.get_key('MaxInstances'):
        new_size = settings.get_key('MaxInstances')
    if new_size < settings.get_key('MinInstances'):
        new_size = settings.get_key('MinInstances')
    if new_size == current_nodes:
        return 'ok', 204
    logging.info(
        "Updating cluster from {} to {} nodes".format(current_nodes, new_size))
    dp.patch_cluster(new_size)
    return 'ok', 204


