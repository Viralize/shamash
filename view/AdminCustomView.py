import flask_admin
from flask_admin.contrib import appengine


class AdminCustomView(flask_admin.contrib.appengine.view.NdbModelView):
    column_list = ['Cluster', 'Region', 'PreemptiblePct',
                   'UpContainerPendingRatio', 'DownYARNMemAvailePct',
                   'UpYARNMemAvailPct', 'MaxInstances', 'MinInstances']
    column_labels = dict(MinInstances='Min Number of Nodes',
                         MaxInstances='Max number of Nodes',
                         Cluster='Cluster Name', Region='Cluster Region',
                         UpContainerPendingRatio='ContainerPendingRatio',
                         UpYARNMemAvailPct='Scale Out % YARNMemoryAvailable',
                         PreemptiblePct='% Preemptible',
                         DownYARNMemAvailePct='Scale In % YARNMemoryAvailable'
                         )
    list_template = 'list.html',
    edit_template = 'edit.html',
    create_template = 'create.html',
    column_descriptions = dict(MinInstances='Min Number of Nodes',
                               MaxInstances='Max number of nodes',
                               Cluster='Cluster Name', Region='Cluster Region',
                               UpContainerPendingRatio='ContainerPendingRatio',
                               UpYARNMemAvailPct='Scale Out YARNMemoryAvailablePercentage',
                               PreemptiblePct='% Preemptible nodes',
                               DownYARNMemAvailePct='Scale In YARNMemoryAvailablePercentage'
                               )
