"""Helper functions for scaling."""
import base64
import json
import logging

from model import settings
from monitoring import metrics
from util import pubsub

SCALING_TOPIC = 'shamash-scaling'


def trigger_scaling(direction):
    """
    Start scaling operation.

    :param direction:
    """
    logging.info('Trigger Scaling %s', direction)
    msg = {'messages': [{'data': base64.b64encode(json.dumps(direction))}]}
    pubsub_client = pubsub.get_pubsub_client()
    try:
        pubsub.publish(pubsub_client, msg, SCALING_TOPIC)
    except pubsub.PubSubException as e:
        logging.error(e)


def should_scale(payload):
    """
    Make a decision to scale or not.

    :param payload:
    :return:
    """
    cluster_settings = None
    data = json.loads(base64.b64decode(payload))
    yarn_memory_available_percentage = data[
        'yarn_memory_available_percentage']
    container_pending_ratio = data['container_pending_ratio']
    number_of_nodes = data['number_of_nodes']
    cluster_name = data['cluster']
    yarn_containers_pending = data['yarn_containers_pending']
    workers = data['worker_nodes']
    preemptible_workers = data['preemptible_workers']
    s = settings.get_cluster_settings(cluster_name)
    for st in s:
        cluster_settings = st
    logging.info(
        'Cluster %s YARNMemAvailPct %s ContainerPendingRatio %s number of '
        'nodes %s', cluster_name, yarn_memory_available_percentage,
        container_pending_ratio, number_of_nodes)
    met = metrics.Metrics(cluster_name)
    met.write_timeseries_value('YARNMemoryAvailablePercentage',
                               100 * yarn_memory_available_percentage)
    met.write_timeseries_value('ContainerPendingRatio',
                               container_pending_ratio)
    met.write_timeseries_value('YarnNodes',
                               str(int(workers) + int(preemptible_workers)))
    met.write_timeseries_value('Workers', workers)
    met.write_timeseries_value('PreemptibleWorkers', preemptible_workers)

    scaling_direction = None
    containerpendingratio = -1
    scale_to = -1
    # No memory is allocated so no needs for more nodes just scale down to the
    # minimum
    # pending containers are waiting....
    if container_pending_ratio > cluster_settings.UpContainerPendingRatio:
        scaling_direction = 'up'
        containerpendingratio = container_pending_ratio
    elif container_pending_ratio < cluster_settings.DownContainerPendingRatio:
        scaling_direction = 'down'
        containerpendingratio = container_pending_ratio
    elif yarn_memory_available_percentage == 1:
        if number_of_nodes > cluster_settings.MinInstances:
            scaling_direction = 'down'
            scale_to = cluster_settings.MinInstances
    # We don't have enough memory lets go up
    elif yarn_memory_available_percentage < cluster_settings.UpYARNMemAvailPct:
        scaling_direction = 'up'
    # we have too much memory  :)
    elif yarn_memory_available_percentage > \
            cluster_settings.DownYARNMemAvailePct:
        scaling_direction = 'down'
    body = {
        'cluster': cluster_name,
        'scaling_direction': scaling_direction,
        'containerpendingratio': containerpendingratio,
        'scale_to': scale_to
    }

    if scaling_direction is not None:
        trigger_scaling(body)
    return 'OK', 204
