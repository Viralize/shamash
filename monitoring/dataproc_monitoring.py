import base64
import json

import googleapiclient.discovery
from google.auth import app_engine

from util import pubsub, settings

SCOPES = ['https://www.googleapis.com/auth/cloud-platform']

MONITORING_TOPIC = 'shamash-monitoring'
credentials = app_engine.Credentials(scopes=SCOPES)


class DataProc:

    def __init__(self):
        self.dataproc = googleapiclient.discovery.build('dataproc', 'v1',
                                                        credentials=credentials)


    def get_cluster_data(self):
        return self.dataproc.projects().regions().clusters().get(
            projectId=settings.get_key("project_id"),
            region=settings.get_key("region"),
            clusterName=settings.get_key("cluster")).execute()


    def get_YARNMemoryAvailablePercentage(self):
        res = self.get_cluster_data()
        if int(res["metrics"]["yarnMetrics"][
                   "yarn-memory-mb-allocated"]) == 0:
            return -1
        return int(res["metrics"]["yarnMetrics"][
                       "yarn-memory-mb-allocated"]) / \
               int(res["metrics"]["yarnMetrics"][
                       "yarn-memory-mb-available"])


    def get_ContainerPendingRatio(self):
        res = self.get_cluster_data()
        yarn_container_allocated = int(
            res["metrics"]["yarnMetrics"][
                "yarn-containers-allocated"])
        if yarn_container_allocated == 0:
            return -1
        return int(res["metrics"]["yarnMetrics"][
                       "yarn-containers-pending"]) / \
               yarn_container_allocated


    def patch_cluster(self, nodes):
        body = json.loads(
            '{"config":{"workerConfig":{"numInstances":%d}}}' % nodes)
        self.dataproc.projects().regions().clusters().patch(
            projectId=settings.get_key("project_id"),
            region=settings.get_key("region"),
            clusterName=settings.get_key("cluster"),
            updateMask='config.worker_config.num_instances',
            body=body).execute()


def check_load():
    dp = DataProc()
    res = str(dp.get_YARNMemoryAvailablePercentage()) + "," + \
          str(dp.get_ContainerPendingRatio())
    msg = {'messages': [{'data': base64.b64encode(res)}]}
    pubsub_client = pubsub.get_pubsub_client()
    pubsub.publish(pubsub_client, msg, MONITORING_TOPIC)
    return 'OK', 204
