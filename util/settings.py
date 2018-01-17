import json

from google.appengine.ext import ndb


class Settings(ndb.Model):
    project_id = ndb.StringProperty()
    region = ndb.StringProperty()
    cluster = ndb.StringProperty()
    ScaleOutYARNMemoryAvailablePercentage = ndb.IntegerProperty()
    ScaleOutContainerPendingRatio = ndb.FloatProperty()
    ScaleInYARNMemoryAvailablePercentage = ndb.IntegerProperty()
    MaxInstances = ndb.IntegerProperty()
    MinInstances = ndb.IntegerProperty()



def get_key(key_name):
    entity = Settings.get_or_insert("settings", project_id="myproject",
                                    region="myregion", cluster="mycluster",
                                    ScaleOutYARNMemoryAvailablePercentage=15,
                                    ScaleOutContainerPendingRatio=0.75,
                                    ScaleInYARNMemoryAvailablePercentage=75,
                                    MaxInstances=10, MinInstances=2,
                                    )
    return getattr(entity, key_name)


def setServiceKey(key_file):
    with open(key_file) as json_data_file:
        data = json.load(json_data_file)
    entity = Settings.get_by_id('settings')
    entity.ServiceKey = json.dumps(data)
    entity.put()


def create_topic():
    #    body = {'topic': dest_topic}
    #   client.projects().subscriptions().create(
    # name=project+'/subscriptions/mysub',body=body).execute()
    return
