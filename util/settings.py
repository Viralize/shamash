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