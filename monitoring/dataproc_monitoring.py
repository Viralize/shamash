import base64
import json
import logging

import googleapiclient.discovery
from google.auth import app_engine
from googleapiclient.errors import HttpError

from util import pubsub, settings

SCOPES = ['https://www.googleapis.com/auth/cloud-platform']

MONITORING_TOPIC = 'shamash-monitoring'
credentials = app_engine.Credentials(scopes=SCOPES)


class DataProcException(Exception):
    def __init__(self, value):
        self.parameter = value


    def __str__(self):
        return repr(self.parameter)


class DataProc:

    def __init__(self):
        self.dataproc = googleapiclient.discovery.build('dataproc', 'v1',
                                                        credentials=credentials)


    def __get_cluster_data(self):
        try:
            return self.dataproc.projects().regions().clusters().get(
                projectId=settings.get_key("project_id"),
                region=settings.get_key("region"),
                clusterName=settings.get_key("cluster")).execute()
        except HttpError as e:
            logging.error(e)
            raise e


    def get_cluster_status(self):
        try:
            cluster_data = self.__get_cluster_data()
            status = cluster_data['status']['state']
        except (HttpError, KeyError) as e:
            raise DataProcException(e)
        return status


    def get_YARNMemoryAvailablePercentage(self):
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
            raise DataProcException(e)
        return status


    def get_ContainerPendingRatio(self):
        try:
            res = self.__get_cluster_data()
            yarn_container_allocated = int(
                res["metrics"]["yarnMetrics"][
                    "yarn-containers-allocated"])
            if yarn_container_allocated == 0:
                return -1
            return int(res["metrics"]["yarnMetrics"][
                           "yarn-containers-pending"]) / yarn_container_allocated
        except (HttpError, KeyError) as e:
            raise DataProcException(e)


    def get_number_of_nodes(self):
        try:
            res = self.__get_cluster_data()
            nodes = int(res["metrics"]["yarnMetrics"][
                            "yarn-nodes-active"])
        except (HttpError, KeyError) as e:
            raise DataProcException(e)
        return nodes


    def patch_cluster(self, nodes):
        body = json.loads(
            '{"config":{"workerConfig":{"numInstances":%d}}}' % nodes)
        try:
            self.dataproc.projects().regions().clusters().patch(
                projectId=settings.get_key("project_id"),
                region=settings.get_key("region"),
                clusterName=settings.get_key("cluster"),
                updateMask='config.worker_config.num_instances',
                body=body).execute()
        except HttpError as e:
            raise DataProcException(e)


def check_load():
    dp = DataProc()
    try:
        res = str(dp.get_YARNMemoryAvailablePercentage()) + "," + str(
            dp.get_ContainerPendingRatio()) + "," + str(
            dp.get_number_of_nodes())
    except DataProcException as e:
        logging.error(e)
        return 'Error', 500
    msg = {'messages': [{'data': base64.b64encode(res)}]}
    pubsub_client = pubsub.get_pubsub_client()
    pubsub.publish(pubsub_client, msg, MONITORING_TOPIC)
    return 'OK', 204
