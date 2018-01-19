"""Interact with pub/sub"""
import logging

from google.auth import app_engine
from googleapiclient import discovery
from googleapiclient.errors import HttpError

from . import settings

PUBSUB_SCOPES = ['https://www.googleapis.com/auth/pubsub',
                 'https://www.googleapis.com/auth/cloud-platform']
credentials = app_engine.Credentials(scopes=PUBSUB_SCOPES)


class PubSubException(Exception):
    """
    Exception class for Pub/Sub functions
    """

    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)


def get_pubsub_client():
    """Get a pubsub client from the API"""
    return discovery.build('pubsub', 'v1',
                           credentials=credentials)


def publish(client, body, topic):
    """Publish a message to a Pub/Sub topic"""
    project = 'projects/{}'.format(settings.get_key('project_id'))
    dest_topic = project + '/topics/' + topic
    try:
        client.projects().topics().publish(topic=dest_topic, body=body).execute()
    except HttpError as e:
        logging.error(e)
        raise PubSubException(e)


def fqrn(resource_type, project, resource):
    """Return a fully qualified resource name for Cloud Pub/Sub."""
    return "projects/{}/{}/{}".format(project, resource_type, resource)


def get_full_subscription_name(project, subscription):
    """Return a fully qualified subscription name."""
    print fqrn('subscriptions', project, subscription)
    return fqrn('subscriptions', project, subscription)


def pull(client, sub, endpoint):
    """Register a listener endpoint """
    subscription = get_full_subscription_name(settings.get_key('project_id'),
                                              sub)
    body = {'pushConfig': {'pushEndpoint': endpoint}}
    try:
        client.projects().subscriptions().modifyPushConfig(
            subscription=subscription,
            body=body).execute()
    except HttpError as e:
        logging.error(e)
        return 'Error', 500
    return 'ok, 204'
