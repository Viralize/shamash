"""Admin view """
import flask_admin
from flask_admin.contrib import appengine
from wtforms import validators
from collections import OrderedDict
from validators import GreaterEqualThan, SmallerEqualThan


class LastUpdatedOrderedDict(OrderedDict):
    """Store items in the order the keys were last added"""

    def __setitem__(self, key, value):
        if key in self:
            del self[key]
        OrderedDict.__setitem__(self, key, value)


class AdminCustomView(flask_admin.contrib.appengine.view.NdbModelView):
    """
    Admin View
    """
    column_dic = LastUpdatedOrderedDict()

    column_dic['Cluster'] = {
        'label': 'Cluster Name',
        'description': 'Google Dataproc Cluster Name'
    }
    column_dic['Region'] = {
        'label': 'Cluster Region',
        'description': 'Cluster Region'
    }
    column_dic['PreemptiblePct'] = {
        'label': '% Preemptible',
        'description': 'The ratio of preemptible workers in Dataproc cluster'
    }
    column_dic['UpContainerPendingRatio'] = {
        'label':
        'Container Pending Ratio',
        'description':
        'The ratio of pending containers allocated to trigger scale out event'
        ' of the cluster'
    }
    column_dic['DownYARNMemAvailePct'] = {
        'label':
        'Scale In % YARNMemoryAvailable',
        'description':
        'The percentage of remaining memory available to YARN to trigger scale'
        ' down'
    }
    column_dic['UpYARNMemAvailPct'] = {
        'label':
        'Scale Out % YARNMemoryAvailable',
        'description':
        'The percentage of remaining memory available to YARN to trigger'
        ' scale out'
    }
    column_dic['MinInstances'] = {
        'label':
        'Min Number of Nodes',
        'description':
        'The least number of workers allowed, even if the target is'
        ' exceeded'
    }
    column_dic['MaxInstances'] = {
        'label':
        'Max number of Nodes',
        'description':
        'The largest number of workers allowed, even if the target is'
        ' exceeded'
    }

    column_list = ([key for key in column_dic])
    column_labels = {
        key: dict(value).get('label')
        for key, value in column_dic.items()
    }
    column_descriptions = {
        key: dict(value).get('description')
        for key, value in column_dic.items()
    }
    list_template = 'list.html',
    edit_template = 'edit.html',
    create_template = 'create.html',

    form_args = column_dic
    form_args['MinInstances']['validators'] = [validators.NumberRange(2)]
    form_args['MaxInstances']['validators'] = [
        validators.NumberRange(2),
        GreaterEqualThan(fieldname='MinInstances')
    ]
    form_args['UpContainerPendingRatio']['validators'] = [
        validators.NumberRange(0),
        SmallerEqualThan(fieldname='DownYARNMemAvailePct')
    ]
    form_args['UpYARNMemAvailPct']['validators'] = [
        validators.NumberRange(0, 100)
    ]
    form_args['PreemptiblePct']['validators'] = [
        validators.NumberRange(0, 100)
    ]
    form_args['DownYARNMemAvailePct']['validators'] = [
        validators.NumberRange(0, 100),
        GreaterEqualThan(fieldname='UpYARNMemAvailPct')
    ]
