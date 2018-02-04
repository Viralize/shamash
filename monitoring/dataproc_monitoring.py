"""Dataproc actions."""
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

CREDENTIALS = app_engine.Credentials(scopes=SCOPES)


class DataProcException(Exception):
    """Exception class for DataProc functions."""

    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)


class DataProc(object):
    """Class for interacting with a Dataproc cluster."""

    def __init__(self, cluster_name):
        self.dataproc = googleapiclient.discovery. \
            build('dataproc', 'v1',
                  credentials=CREDENTIALS)
        self.cluster_name = cluster_name
        self.project_id = utils.get_project_id()
        s = settings.get_cluster_settings(cluster_name)
        if s.count(1) == 1:
            for st in s:
                self.cluster_settings = st
        else:
            raise DataProcException('Cluster not found!')

    def __get_cluster_data(self):
        """Get a json with cluster data/status."""

        @backoff.on_exception(
            backoff.expo, HttpError, max_tries=8, giveup=utils.fatal_code)
        def _do_request():
            return self.dataproc.projects().regions().clusters().get(
                projectId=utils.get_project_id(),
                region=self.cluster_settings.Region,
                clusterName=self.cluster_name).execute()

        @backoff.on_predicate(backoff.expo)
        def _validate_json(js):
            if 'metrics' not in js:
                return False
            if 'yarnMetrics' not in js['metrics']:
                return False
            return True

        try:
            res = _do_request()
        except HttpError as e:
            logging.error(e)
            raise e
        _validate_json(res)
        return res

    def get_cluster_status(self):
        """Get status of the cluster.running updating etc."""
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
        yarn-memory-mb-available + yarn-memory-mb-allocated = Total cluster
         memory.
         yarn_memory_mb_available / Total Cluster Memory

        """
        try:
            res = self.__get_cluster_data()
            yarn_memory_mb_allocated = int(res['metrics']['yarnMetrics'][
                'yarn-memory-mb-allocated'])
            yarn_memory_mb_available = int(res['metrics']['yarnMetrics'][
                'yarn-memory-mb-available'])
            total_memory = yarn_memory_mb_allocated + yarn_memory_mb_available

            return int(yarn_memory_mb_available) / int(total_memory)
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

            yarn_containers_pending = int(res['metrics']['yarnMetrics'][
                'yarn-containers-pending'])

            yarn_container_allocated = int(res['metrics']['yarnMetrics'][
                'yarn-containers-allocated'])

            if yarn_container_allocated == 0:
                return yarn_containers_pending

            return yarn_containers_pending / yarn_container_allocated
        except (HttpError, KeyError) as e:
            logging.error(e)
            raise DataProcException(e)

    def get_number_of_nodes(self):
        """Get the number of active nodes in a cluster."""
        try:
            res = self.__get_cluster_data()
            nodes = int(res['metrics']['yarnMetrics']['yarn-nodes-active'])
        except (HttpError, KeyError) as e:
            logging.error(e)
            raise DataProcException(e)
        return nodes

    def get_number_of_workers(self):
        """Get the number of 'real workers."""
        try:
            res = self.__get_cluster_data()
        except (HttpError, KeyError) as e:
            logging.error(e)
            raise DataProcException(e)
        else:
            nodes = int(res['config']['workerConfig']['numInstances'])
            return nodes

    def get_yarn_containers_pending(self):
        """
        Get the number of pending containers.
        :return:
        """
        try:
            res = self.__get_cluster_data()
        except HttpError as e:
            logging.error(e)
            raise DataProcException(e)
        except KeyError as e:
            logging.info(e)
        else:
            pending = int(res['metrics']['yarnMetrics'][
                'yarn-containers-pending'])
            return pending

    def get_number_of_preemptible_workers(self):
        """Get the number of 'real workers."""
        nodes = 0
        if self.cluster_settings.PreemptiblePct == 0:
            return 0
        if self.get_number_of_workers() - self.get_number_of_nodes() == 0:
            return 0
        try:
            res = self.__get_cluster_data()
        except HttpError as e:
            logging.error(e)
            raise DataProcException(e)
        if res.get('config') is not None:
            if res.get('config').get('secondaryWorkerConfig') is not None:
                nodes = res.get('config').get('secondaryWorkerConfig').get(
                    'numInstances')
                if nodes is None:
                    nodes = 0
        return nodes

    def patch_cluster(self, worker_nodes, preemptible_nodes):
        """Update number of nodes in a cluster."""
        logging.debug("Wants %s %s got %s %s", worker_nodes, preemptible_nodes,
                      self.get_number_of_workers(),
                      self.get_number_of_preemptible_workers())

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

        # Wait for cluster
        _is_cluster_running()
        if self.get_number_of_workers() != worker_nodes:
            body = json.loads('{"config":{"workerConfig":{"numInstances":%d}}}' %
                              worker_nodes)
            update_mask = 'config.worker_config.num_instances'
            try:
                _do_request(update_mask)
            except HttpError as e:
                raise DataProcException(e)

        if self.get_number_of_preemptible_workers() == preemptible_nodes:
            return 'ok', 204
        body = json.loads(
            '{"config":{"secondaryWorkerConfig":{"numInstances":%d}}}' %
            preemptible_nodes)
        update_mask = 'config.secondary_worker_config.num_instances'
        _is_cluster_running()
        try:
            _do_request(update_mask)
        except HttpError as e:
            raise DataProcException(e)
        return 'ok', 200

    def check_load(self):
        """Get the current cluster metrics and publish them to pub/sub."""
        try:
            monitor_data = {
                'cluster': self.cluster_name,
                'yarn_memory_available_percentage':
                float(self.get_yarn_memory_available_percentage()),
                'container_pending_ratio':
                float(self.get_container_pending_ratio()),
                'number_of_nodes': int(self.get_number_of_nodes()),
                'worker_nodes': int(self.get_number_of_workers()),
                'yarn_containers_pending':
                int(self.get_yarn_containers_pending()),
                'preemptible_workers': self.get_number_of_preemptible_workers(),
                'workers': self.get_number_of_workers()
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

        logging.debug('Monitor data for %s is %s', self.cluster_name,
                      json.dumps(monitor_data))
        pubsub_client = pubsub.get_pubsub_client()
        try:
            pubsub.publish(pubsub_client, msg, MONITORING_TOPIC)
        except pubsub.PubSubException as e:
            logging.error(e)
            return 'Error', 500
        return 'OK', 204

    def get_memory_data(self):
        """
         Retrieve yarn_memory_mb_allocated, yarn_memory_mb_pending.

        :return: yarn_memory_mb_allocated, yarn_memory_mb_pending
        """
        try:
            res = self.__get_cluster_data()
        except (HttpError, KeyError) as e:
            logging.error(e)
            raise DataProcException(e)
        else:
            yarn_memory_mb_allocated = int(res['metrics']['yarnMetrics'][
                'yarn-memory-mb-allocated'])
            yarn_memory_mb_pending = int(res['metrics']['yarnMetrics'][
                'yarn-memory-mb-pending'])

            return yarn_memory_mb_allocated, yarn_memory_mb_pending

    def get_container_data(self):
        """
        Retrieve container  status.

        :return: yarn_containers_allocated, yarn_containers_pending
        """
        try:
            res = self.__get_cluster_data()
        except (HttpError, KeyError) as e:
            logging.error(e)
            raise DataProcException(e)
        else:
            yarn_containers_allocated = int(res['metrics']['yarnMetrics'][
                'yarn-containers-allocated'])
            yarn_containers_pending = int(res['metrics']['yarnMetrics'][
                'yarn-containers-pending'])

            return yarn_containers_allocated, yarn_containers_pending
