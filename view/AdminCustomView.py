"""Admin view."""
from collections import OrderedDict

import flask_admin
from flask_admin.contrib import appengine
from wtforms import validators

from view.validators import GreaterEqualThan, SmallerEqualThan


class LastUpdatedOrderedDict(OrderedDict):
    """Store items in the order the keys were last added."""

    def __setitem__(self, key, value):
        if key in self:
            del self[key]
        OrderedDict.__setitem__(self, key, value)


class AdminCustomView(flask_admin.contrib.appengine.view.NdbModelView):
    """Admin View."""

    column_dic = LastUpdatedOrderedDict()

    column_dic.update({
        'Enabled': {
            'label': 'Enable auto scaling',
            'description': 'Is auto scaling enabled'
        },
        'Cluster': {
            'label': 'Cluster Name',
            'description': 'Google Dataproc Cluster Name'
        },
        'Region': {
            'label': 'Cluster Region',
            'description': 'Cluster Region'
        },
        'AddRemoveUpDelta': {
            'label': 'Add/Remove delta',
            'description':
            'The number of nodes to add. If 0 Shamash will calulate this automatically',
            'validators': [
                validators.NumberRange(min=0),
                SmallerEqualThan(fieldname='MaxInstances')
            ]
        },
        'AddRemoveDownDelta': {
            'label': 'Remove delta',
            'description':
            'The number of nodes to remove. If 0 Shamash will calulate this automatically',
            'validators': [
                validators.NumberRange(min=0),
                SmallerEqualThan(fieldname='MaxInstances')
            ]
        },
        'UseMemoryForScaling': {
            'label': 'Use Memory metrics for scaling',
            'description':
            'Should Shamsh use memory metrics for scaling decision'
        },
        'PreemptiblePct': {
            'label': '% Preemptible',
            'description':
            'The ratio of preemptible workers in Dataproc cluster',
            'validators': [validators.NumberRange(0, 100)]
        },
        'UpContainerPendingRatio': {
            'label': 'Container Pending Ratio',
            'description':
            'The ratio of pending containers allocated to trigger scale '
            'out event of the cluster',
            'validators': [
                validators.NumberRange(0),
            ]
        },
        'DownContainerPendingRatio': {
            'label': 'Container Pending Ratio',
            'description':
                'The ratio of pending containers allocated to trigger scale '
                'in event of the cluster',
            'validators': [
                validators.NumberRange(0),
            ]
        },

        'DownYARNMemAvailePct': {
            'label': 'Scale In % YARNMemoryAvailable',
            'description':
            'The percentage of remaining memory available to YARN to trigger '
            'scale down',
            'validators': [
                validators.NumberRange(0, 100),
                GreaterEqualThan(fieldname='UpYARNMemAvailPct')
            ]
        },
        'UpYARNMemAvailPct': {
            'label': 'Scale Out % YARNMemoryAvailable',
            'description':
            'The percentage of remaining memory available to YARN to trigger'
            ' scale out',
            'validators': [validators.NumberRange(0, 100)]
        },
        'MinInstances': {
            'label': 'Min Number of Nodes',
            'description':
            'The least number of workers allowed, even if the target is'
            ' exceeded',
            'validators': [validators.NumberRange(2)]
        },
        'MaxInstances': {
            'label': 'Max number of Nodes',
            'description':
            'The largest number of workers allowed, even if the target is'
            ' exceeded',
            'validators': [
                validators.NumberRange(2),
                GreaterEqualThan(fieldname='MinInstances')
            ]
        },
        'GracefulDecommissionTimeout': {
            'label': 'Graceful Decommission Timeout',
            'description': 'Graceful Decommission Timeout in minutes',
            'validators': [validators.NumberRange(0, 1440),]
        },
    })

    column_list = ([key for key in column_dic])
    column_labels = {
        key: dict(value).get('label')
        for key, value in column_dic.items()
    }
    column_descriptions = {
        key: dict(value).get('description')
        for key, value in column_dic.items()
    }

    list_template = 'list.html'
    edit_template = 'edit.html'
    create_template = 'create.html'

    form_args = column_dic
