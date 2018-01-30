"""Dataproc actions """
import base64
import json
import logging

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

    def __init__(self, cluster_name):
        self.dataproc = googleapiclient.discovery. \
            build('dataproc', 'v1',
                  credentials=credentials)
        self.cluster_name = cluster_name
        self.project_id = utils.get_project_id()
        s = settings.get_cluster_settings(cluster_name)
        if s.count(1) == 1:
            for st in s:
                self.cluster_settings = st
        else:
            raise DataProcException("Cluster not found!")

    def __get_cluster_data(self):
        """Get a json with cluster data/status."""

        @backoff.on_exception(
            backoff.expo, HttpError, max_tries=8, giveup=utils.fatal_code)
        def _do_request():
            return self.dataproc.projects().regions().clusters().get(
                projectId=utils.get_project_id(),
                region=self.cluster_settings.Region,
                clusterName=self.cluster_name).execute()

        try:
            return _do_request()
        except HttpError as e:
            logging.error(e)
            raise e

    def get_cluster_status(self):
        """Get status of the cluster.running updating etc"""
        try:
            cluster_data = self.__get_cluster_data()

        except (HttpError, KeyError) as e:
            logging.error(e)
            raise DataProcException(e)
        else:
            status = cluster_data['status']['state']
            return status

    def get_yarn_memory_available_percentage(self):
        """The percentage of remaining memory available to YARN
        yarn-memory-mb-allocated/yarn-memory-mb-available
        """
        try:
            res = self.__get_cluster_data()
            if int(res["metrics"]["yarnMetrics"][
                    "yarn-memory-mb-allocated"]) == 0:
                return -1
            if int(res["metrics"]["yarnMetrics"][
                    "yarn-memory-mb-available"]) == 0:
                return 0
            return int(res["metrics"][
                "yarnMetrics"]["yarn-memory-mb-allocated"]) / int(
                    res["metrics"]["yarnMetrics"]["yarn-memory-mb-available"])
        except (HttpError, KeyError) as e:
            logging.error(e)
            raise DataProcException(e)

    def get_container_pending_ratio(self):
        """The ratio of pending containers to containers allocated
        (ContainerPendingRatio = ContainerPending / ContainerAllocated).
        If ContainerAllocated = 0, then ContainerPendingRatio =
        ContainerPending. The value of ContainerPendingRatio represents
        a number, not a percentage.
        """
        try:
            res = self.__get_cluster_data()
            yarn_container_allocated = int(
                res["metrics"]["yarnMetrics"]["yarn-containers-allocated"])
            yarn_containers_pending = int(
                res["metrics"]["yarnMetrics"]["yarn-containers-pending"])
            if yarn_container_allocated == 0:
                return yarn_containers_pending
            return yarn_containers_pending / yarn_container_allocated
        except (HttpError, KeyError) as e:
            logging.error(e)
            raise DataProcException(e)

    def get_number_of_nodes(self):
        """Get the number of active nodes in a cluster"""
        try:
            res = self.__get_cluster_data()
            nodes = int(res["metrics"]["yarnMetrics"]["yarn-nodes-active"])
        except (HttpError, KeyError) as e:
            logging.error(e)
            raise DataProcException(e)
        return nodes

    def get_number_of_workers(self):
        """Get the number of 'real workers"""
        nodes = 0
        try:
            res = self.__get_cluster_data()
        except (HttpError, KeyError) as e:
            logging.error(e)
            raise DataProcException(e)
        else:
            nodes = int(res['config']["workerConfig"]["numInstances"])
            return nodes

    def get_yarn_containers_pending(self):
        """
        Get the number of pending containers.
        :return:
        """
        pending = 0
        try:
            res = self.__get_cluster_data()
        except HttpError as e:
            logging.error(e)
            raise DataProcException(e)
        except KeyError as e:
            logging.info(e)
        else:
            pending = int(
                res["metrics"]["yarnMetrics"]["yarn-containers-pending"])
            return pending

    def get_number_of_preemptible_workers(self):
        """Get the number of 'real workers"""
        if self.cluster_settings.PreemptiblePct == 0:
            return 0
        if self.get_number_of_workers() - self.get_number_of_nodes() == 0:
            return 0
        nodes = 0
        try:
            res = self.__get_cluster_data()
        except HttpError as e:
            logging.error(e)
            raise DataProcException(e)
        except KeyError as e:
            logging.info(e)
        else:
            nodes = int(res['config']["secondaryWorkerConfig"]["numInstances"])
            return nodes

    def patch_cluster(self, worker_nodes, preemptible_nodes):
        """Update number of nodes in a cluster"""
        """Wait for cluster"""
        logging.debug("Wants {} {} got {} {}".format(
            worker_nodes, preemptible_nodes, self.get_number_of_workers(),
            self.get_number_of_preemptible_workers()))

        @backoff.on_exception(
            backoff.expo, HttpError, max_tries=8, giveup=utils.fatal_code)
        def _do_request(mask):
            self.dataproc.projects().regions().clusters().patch(
                projectId=self.project_id,
                region=self.cluster_settings.Region,
                clusterName=self.cluster_name,
                updateMask=mask,
                body=body).execute()

        @backoff.on_predicate(backoff.expo)
        def _is_cluster_running():
            return self.get_cluster_status().lower() == 'running'

        _is_cluster_running()
        if self.get_number_of_workers() != worker_nodes:
            body = json.loads('{"config":{"workerConfig":{"numInstances":%d}}}'
                              % worker_nodes)
            update_mask = 'config.worker_config.num_instances'
            try:
                _do_request(update_mask)
            except HttpError as e:
                raise DataProcException(e)
        if self.get_number_of_preemptible_workers(
        ) == preemptible_nodes:
            return
        body = json.loads(
            '{"config":{"secondaryWorkerConfig":{"numInstances":%d}}}' %
            preemptible_nodes)
        update_mask = 'config.secondary_worker_config.num_instances'
        _is_cluster_running()
        try:
            _do_request(update_mask)
        except HttpError as e:
            raise DataProcException(e)

    def check_load(self):
        """Get the current cluster metrics and publish them to pub/sub """
        try:
            monitor_data = {
                "cluster":
                self.cluster_name,
                "yarn_memory_available_percentage":
                int(self.get_yarn_memory_available_percentage()),
                "container_pending_ratio":
                float(self.get_container_pending_ratio()),
                "number_of_nodes":
                int(self.get_number_of_nodes()),
                "worker_nodes":
                int(self.get_number_of_workers()),
                'yarn_containers_pending':
                int(self.get_yarn_containers_pending())
            }
            if self.cluster_settings.PreemptiblePct != 0:
                monitor_data['preemptible_nodes'] = int(
                    self.get_number_of_preemptible_workers())
        except DataProcException as e:
            logging.error(e)
            return 'Error', 500
        msg = {
            'messages': [{
                'data': base64.b64encode(json.dumps(monitor_data))
            }]
        }
        logging.debug("Monitor data for {} is |{}".format(
            self.cluster_name, json.dumps(monitor_data)))
        pubsub_client = pubsub.get_pubsub_client()
        try:
            pubsub.publish(pubsub_client, msg, MONITORING_TOPIC)
        except pubsub.PubSubException as e:
            logging.error(e)
            return 'Error', 500
        return 'OK', 204
