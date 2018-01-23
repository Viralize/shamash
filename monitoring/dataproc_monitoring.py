"""Dataproc actions """
import base64
import json
import logging
import time

import backoff
import googleapiclient.discovery
from google.auth import app_engine
from googleapiclient.errors import HttpError

from model import settings
from util import pubsub, utils

SCOPES = ['https://www.googleapis.com/auth/cloud-platform']

MONITORING_TOPIC = 'shamash-monitoring'
credentials = app_engine.Credentials(scopes=SCOPES)


class DataProcException(Exception):
    """
    Exception class for DataProc functions
    """

    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)


class DataProc:
    """
    Class for interacting with a Dataproc cluster
    """

    def __init__(self, cluster):
        self.dataproc = googleapiclient.discovery. \
            build('dataproc', 'v1',
                  credentials=credentials)
        self.cluster = cluster
        self.project_id = utils.get_project_id()
        s = settings.get_cluster_settings(cluster)
        for st in s:
            self.cluster_settings = st

    def __get_cluster_data(self):
        """Get a json with cluster data/status."""

        @backoff.on_exception(backoff.expo,
                              HttpError,
                              max_tries=3, giveup=utils.fatal_code)
        def _do_request():
            return self.dataproc.projects().regions().clusters().get(
                projectId=utils.get_project_id(),
                region=self.cluster_settings.Region,
                clusterName=self.cluster).execute()

        try:
            _do_request()
        except HttpError as e:
            logging.error(e)
            raise e

    def get_cluster_status(self):
        """Get status of the cluster.running updating etc"""
        try:
            cluster_data = self.__get_cluster_data()
            status = cluster_data['status']['state']
        except (HttpError, KeyError) as e:
            logging.error(e)
            raise DataProcException(e)
        return status

    def get_yarn_memory_available_percentage(self):
        """Calculate cluster available memory %
        yarn-memory-mb-allocated/yarn-memory-mb-available"""
        try:
            res = self.__get_cluster_data()
            if int(res["metrics"]["yarnMetrics"][
                       "yarn-memory-mb-allocated"]) == 0:
                return -1
            if int(res["metrics"]["yarnMetrics"][
                       "yarn-memory-mb-available"]) == 0:
                return 0
            return int(res["metrics"]["yarnMetrics"][
                           "yarn-memory-mb-allocated"]) / \
                   int(res["metrics"]["yarnMetrics"][
                           "yarn-memory-mb-available"])
        except (HttpError, KeyError) as e:
            logging.error(e)
            raise DataProcException(e)

    def get_container_pending_ratio(self):
        """Calculate the ratio between allocated and pending containers"""
        try:
            res = self.__get_cluster_data()
            yarn_container_allocated = int(
                res["metrics"]["yarnMetrics"]["yarn-containers-allocated"])
            if yarn_container_allocated == 0:
                return -1
            return int(res["metrics"]["yarnMetrics"][
                           "yarn-containers-pending"]) / \
                   yarn_container_allocated
        except (HttpError, KeyError) as e:
            logging.error(e)
            raise DataProcException(e)

    def get_number_of_nodes(self):
        """Get the number of active nodes in a cluster"""
        try:
            res = self.__get_cluster_data()
            nodes = int(res["metrics"]["yarnMetrics"][
                            "yarn-nodes-active"])
        except (HttpError, KeyError) as e:
            logging.error(e)
            raise DataProcException(e)
        return nodes

    def get_number_of_workers(self):
        """Get the number of 'real workers"""
        try:
            nodes = 0
            res = self.__get_cluster_data()
            nodes = int(res['config']["workerConfig"]["numInstances"])
        except (HttpError, KeyError) as e:
            logging.error(e)
            raise DataProcException(e)
        return nodes

    def get_number_of_preemptible_workers(self):
        """Get the number of 'real workers"""
        try:
            nodes = 0
            res = self.__get_cluster_data()
            nodes = int(res['config']["secondaryWorkerConfig"]["numInstances"])
        except HttpError as e:
            logging.error(e)
            raise DataProcException(e)
        except KeyError as e:
            logging.info(e)
        return nodes

    def patch_cluster(self, worker_nodes, preemptible_nodes):
        """Update number of nodes in a cluster"""
        try:
            body = json.loads(
                '{"config":{"secondaryWorkerConfig":{"numInstances":%d}}}' % preemptible_nodes)
            self.dataproc.projects().regions().clusters().patch(
                projectId=self.project_id,
                region=self.cluster_settings.Region,
                clusterName=self.cluster,
                updateMask='config.secondary_worker_config.num_instances',
                body=body).execute()
            """Wait for cluster"""
            while self.get_cluster_status().lower() != 'running':
                time.sleep(1)
            body = json.loads(
                '{"config":{"workerConfig":{"numInstances":%d}}}' % worker_nodes)
        except HttpError as e:
            raise DataProcException(e)

        @backoff.on_exception(backoff.expo,
                              HttpError,
                              max_tries=3, giveup=utils.fatal_code)
        def _do_request():
            self.dataproc.projects().regions().clusters().patch(
                projectId=self.project_id,
                region=self.cluster_settings.Region,
                clusterName=self.cluster,
                updateMask='config.worker_config.num_instances',
                body=body).execute()

        try:
            _do_request()
        except HttpError as e:
            raise DataProcException(e)


def check_load():
    """Get the current cluster metrics and publish them to pub/sub"""
    clusters = settings.get_all_clusters_settings()
    for cluster in clusters.iter():
        dp = DataProc(cluster.Cluster)
        try:
            monitor_data = {
                "cluster": cluster.Cluster,
                "yarn_memory_available_percentage": int(
                    dp.get_yarn_memory_available_percentage()),
                "container_pending_ratio": float(
                    dp.get_container_pending_ratio()),
                "number_of_nodes": int(dp.get_number_of_nodes()),
                "worker_nodes": int(dp.get_number_of_workers()),
                'preemptible_nodes': int(
                    dp.get_number_of_preemptible_workers())
            }
        except DataProcException as e:
            logging.error(e)
            return 'Error', 500
        msg = {
            'messages': [{'data': base64.b64encode(json.dumps(monitor_data))}]}
        pubsub_client = pubsub.get_pubsub_client()
        try:
            pubsub.publish(pubsub_client, msg, MONITORING_TOPIC)
        except pubsub.PubSubException as e:
            logging.error(e)
            return 'Error', 500
    return 'OK', 204
