"""Admin view """
import flask_admin
from flask_admin.contrib import appengine
from wtforms import validators, Form

from validators import GreaterEqualThan


class AdminCustomView(flask_admin.contrib.appengine.view.NdbModelView):
    """
    Admin View
    """
    column_list = [
        'Cluster', 'Region', 'PreemptiblePct', 'UpContainerPendingRatio',
        'DownYARNMemAvailePct', 'UpYARNMemAvailPct', 'MaxInstances',
        'MinInstances'
    ]
    column_labels = dict(
        MinInstances='Min Number of Nodes',
        MaxInstances='Max number of Nodes',
        Cluster='Cluster Name',
        Region='Cluster Region',
        UpContainerPendingRatio='Container Pending Ratio',
        UpYARNMemAvailPct='Scale Out % YARNMemoryAvailable',
        PreemptiblePct='% Preemptible',
        DownYARNMemAvailePct='Scale In % YARNMemoryAvailable')
    column_editable_list = [
        'MinInstances', 'MaxInstances', 'UpContainerPendingRatio',
        'UpYARNMemAvailPct', 'PreemptiblePct', 'DownYARNMemAvailePct'
    ]
    list_template = 'list.html',
    edit_template = 'edit.html',
    create_template = 'create.html',
    column_descriptions = dict(
        MinInstances=
        'The least number of workers the cluster will contain, even if the'
        ' target is not met',
        MaxInstances=
        'The largest number of workers allowed, even if the target is'
        ' exceeded',
        Cluster='Google Dataproc Cluster Name',
        Region='Cluster Region',
        UpContainerPendingRatio=
        'The ratio of pending containers allocated to trigger scale out event'
        ' of the cluster',
        UpYARNMemAvailPct=
        'The percentage of remaining memory available to YARN to trigger'
        ' scale out',
        PreemptiblePct='The ratio of preemptible workers in Dataproc cluster',
        DownYARNMemAvailePct=
        'The percentage of remaining memory available to YARN to trigger scale'
        ' down')
    form_args = {
        'MinInstances': {
            'label': 'Min Number of Nodes',
            'validators': [validators.NumberRange(2)]
        },
        'MaxInstances': {
            'label':
            'Max number of Nodes',
            'validators': [
                validators.NumberRange(2),
                GreaterEqualThan(fieldname='Min Number of Nodes')
            ]
        },
        'Cluster': {
            'label': 'Cluster Name'
        },
        'Region': {
            'label': 'Cluster Region'
        },
        'UpContainerPendingRatio': {
            'label': 'Container Pending Ratio',
            'validators': [validators.NumberRange(0)]
        },
        'UpYARNMemAvailPct': {
            'label': 'Scale Out % YARNMemoryAvailable',
            'validators': [validators.NumberRange(0, 100)]
        },
        'PreemptiblePct': {
            'label': '% Preemptible',
            'validators': [validators.NumberRange(0, 100)]
        },
        'DownYARNMemAvailePct': {
            'label': 'Scale In % YARNMemoryAvailable',
            'validators': [validators.NumberRange(0, 100)]
        }
    }
