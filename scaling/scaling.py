"""Handle scaling """
import base64
import json
import logging

import numpy as np

import scale_calculations
from model import settings
from monitoring import dataproc_monitoring, metrics
from util import pubsub

SCALING_TOPIC = 'shamash-scaling'


def trigger_scaling(direction):
    """
    Start scaling operation
    :param direction:
    """
    logging.info("Trigger Scaling {}".format(direction))
    msg = {'messages': [{'data': base64.b64encode(json.dumps(direction))}]}
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
    yarn_containers_pending = data['yarn_containers_pending']
    s = settings.get_cluster_settings(cluster_name)
    for st in s:
        cluster_settings = st
    logging.info(
        "Cluster {} YARNMemAvailPct {} ContainerPendingRatio {}".format(
            cluster_name,
            yarn_memory_available_percentage, container_pending_ratio,
            number_of_nodes))
    met = metrics.Metrics(cluster_name)
    met.write_timeseries_value('YARNMemoryAvailablePercentage',
                               yarn_memory_available_percentage)
    met.write_timeseries_value('ContainerPendingRatio',
                               container_pending_ratio)
    met.write_timeseries_value('YarnNodes', number_of_nodes)

    scaling_direction = None
    containerpendingratio = -1
    scale_to = -1
    """No memory is allocated so no needs for more nodes just scale down to the
     minimum"""
    if yarn_memory_available_percentage == -1:
        if number_of_nodes > cluster_settings.MinInstances:
            scaling_direction = "down"
            scale_to = cluster_settings.MinInstances
    # We don't have enough memory lets go up
    elif yarn_memory_available_percentage < cluster_settings.UpYARNMemAvailPct:
        scaling_direction = "up"
    # pending containers are waiting....
    elif container_pending_ratio > cluster_settings.UpContainerPendingRatio:
        scaling_direction = "up"
        containerpendingratio = container_pending_ratio
    # we have too much memory  :)
    elif yarn_memory_available_percentage > \
            cluster_settings.DownYARNMemAvailePct:
        scaling_direction = "down"
    body = {
        "cluster": cluster_name,
        "scaling_direction": scaling_direction,
        "containerpendingratio": containerpendingratio,
        "scale_to": scale_to
    }
    if scaling_direction == 'down' and yarn_containers_pending == 0:
        scaling_direction = None

    if scaling_direction is not None:
        trigger_scaling(body)
    return 'OK', 204


def calc_slope(minuets, cluster):
    """
    calculate the slope of available memory change
    :param minuets, cluster:
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
               preemptiblepct, cluster_name, containerpendingratio):
    """
    How many nodes to add
    :param current_worker_nodes, current_preemptible_nodes,
               preemptiblepct, cluster_name:
    :return:
    """
    new_workers = current_worker_nodes
    new_preemptibel = current_preemptible_nodes
    current_preemptible_nodes = max(current_preemptible_nodes, 1)
    if containerpendingratio != -1:
        new_workers = current_worker_nodes + (
                1 - preemptiblepct) * current_worker_nodes * (
                              1 / containerpendingratio)
        new_preemptibel = current_preemptible_nodes + (
                preemptiblepct / 100) * current_preemptible_nodes * (
                                  1 / containerpendingratio)
    else:
        sl = calc_slope(60, cluster_name)
        if sl != 0:
            slope = (1 / sl)
            logging.info('Slope is {}'.format(slope))
            if slope != 0:
                new_workers = current_worker_nodes + slope * (
                        1 - (preemptiblepct / 100))
                new_preemptibel = current_preemptible_nodes + slope * (
                        preemptiblepct / 100)
                logging.info(new_preemptibel)
    logging.info(
        "Scaling to workers   {} preemptibel {} ".format(new_workers,
                                                         new_preemptibel))
    return int(new_workers), int(new_preemptibel)


def do_scale(payload):
    """
    Perform the scaling
    :return:
    """
    data = json.loads(base64.b64decode(payload))
    dp = dataproc_monitoring.DataProc(data['cluster'])
    s = settings.get_cluster_settings(data['cluster'])
    for st in s:
        cluster_settings = st
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

    if preemptiblepct != 100:
        ratio = preemptiblepct / (100 - preemptiblepct)
    else:
        ratio = -1
    # just go to the minimum
    if data['scale_to'] != -1:
        new_workers = cluster_settings.MinInstances
        new_preemptibel = 0
    # no more memory lets get some lets at 4 nodes
    elif dp.get_yarn_memory_available_percentage() == 0:
        add_more = 4
        scale_ratio = (float(cluster_settings.PreemptiblePct) / 100.0)

        new_workers = int(
            round(((1 - scale_ratio) * add_more) + current_worker_nodes))
        new_preemptibel = int(
            round(((scale_ratio * add_more) + current_preemptible_nodes)))

        if (new_preemptibel + new_workers) > cluster_settings.MaxInstances:
            new_workers, new_preemptibel = scale_calculations.calc_max_nodes_combination(
                cluster_settings.MaxInstances,
                ratio,
                cluster_settings.MinInstances)

    else:
        new_workers, new_preemptibel = calc_scale(current_worker_nodes,
                                                  current_preemptible_nodes,
                                                  preemptiblepct,
                                                  data['cluster'],
                                                  data[
                                                      'containerpendingratio'])
        # check boundaries
        # make sure the we have the minimum number of workers
        if new_workers < cluster_settings.MinInstances:
            new_workers = cluster_settings.MinInstances
        # check upper boundary
        if preemptiblepct != 100:
            ratio = preemptiblepct / (100 - preemptiblepct)
        else:
            ratio = -1
        if (new_preemptibel + new_workers) > cluster_settings.MaxInstances:
            new_workers, new_preemptibel = scale_calculations.calc_max_nodes_combination(
                cluster_settings.MaxInstances,
                ratio,
                cluster_settings.MinInstances)

    # No need to change the cluster
    if (new_preemptibel + new_workers) == current_nodes:
        return 'Not Modified', 200
    # make sure that we have at least on preemptibl node
    if preemptiblepct != 0:
        if current_preemptible_nodes == 0:
            new_preemptibel = 1
            new_workers = new_workers - 1
    logging.info(
        "Updating cluster from {} to {} nodes".format
        (current_nodes,
         new_preemptibel + new_workers))
    # do the scaling
    # if going down defere
    try:
        dp.patch_cluster(new_workers, new_preemptibel)
    except dataproc_monitoring.DataProcException as e:
        logging.error(e)
        return 'Error', 500
    return 'ok', 204
