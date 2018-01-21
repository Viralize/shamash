""""Settings"""
import googleapiclient.discovery
from google.appengine.api import app_identity
from google.appengine.ext import ndb
from google.auth import app_engine

SCOPES = ['https://www.googleapis.com/auth/cloud-platform']
credentials = app_engine.Credentials(scopes=SCOPES)


def get_regions():
    """
    get all available regions
    :return: all regions
    """
    compute = googleapiclient.discovery.build('compute', 'v1')
    request = compute.regions().list(
        project=app_identity.get_application_id())

    response = request.execute()
    rg = []
    for region in response['items']:
        # TODO: Change code below to process each `region` resource:
        rg.append(region['description'])
    rg.append('global')
    rg.sort()
    return rg


class Settings(ndb.Model):
    """
    Seeting managmnet for Shamash
    """
    Cluster = ndb.StringProperty(indexed=True)
    Region = ndb.StringProperty(choices=get_regions(), default='us-east1')
    UpYARNMemAvailPct = ndb.IntegerProperty(default=75)
    DownYARNMemAvailePct = ndb.IntegerProperty(default=15)
    UpContainerPendingRatio = ndb.FloatProperty(default=0.75)
    PreemptiblePct = ndb.IntegerProperty(default=80)
    MaxInstances = ndb.IntegerProperty(default=10)
    MinInstances = ndb.IntegerProperty(default=2)


def get_cluster_settings(cluser_name):
    """

    :param cluser_name:
    :return:
    """
    return Settings.query(Settings.Cluster == cluser_name)


def get_all_clusters_settings():
    """
    get all entities of setting kind
    :return:
    """
    return Settings.query()