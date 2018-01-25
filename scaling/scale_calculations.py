def calc_max_nodes_combination(max_nodes, ratio, min_nodes):
    """

    :param max_nodes:
    :param ratio:
    :param min_nodes:

    MaxNodes - workers + preemptable =0
    preemptable-workers*Ratio=0
    preemptable-workers*Ratio =MaxNodes - workers + preemptable
    workers(Ratio+1) = MaxNodes
    workers = MaxNodes/(Ratio+1)
    """
    if ratio != -1:
        workers = max_nodes / (ratio + 1)
        preemptable = max_nodes - workers
    else:
        workers = min_nodes
        preemptable = max_nodes - min_nodes
    return int(round(workers)), int(round(preemptable))
