# Work in Progress
# FIXME
Deploy run `./deploy.sh project-id`

For local development run `dev_appserver.py --log_level=debug app.yaml
` you will need a config.json file

`{
  "project": "project-id"
}`
# END OF FIXME
# Shamash - Autoscaling for Dataproc
Shamash is a service for autoscaling Cloud DataProc on Google Cloude Platform(GCP).
treams

[Shamash](https://www.wikiwand.com/en/Shamash) was the god of justice in Babylonia and Assyria, just like
the Shamash autoscaler whose job is to continuously maintain a tradeoff between costs and
performance.
![](static/Shamash.png)

## Background
Cloud Dataproc is a fast, easy-to-use, fully-managed cloud service for running Apache Spark and Apache Hadoop clusters in a simpler, more cost-efficient way. Operations that used to take hours or days take seconds or minutes instead, and you pay only for the resources you use (with per-second billing). Cloud Dataproc also easily integrates with other Google Cloud Platform (GCP) services, giving you a powerful and complete platform for data processing, analytics and machine learning.
Due to different usage patterns (e.g., high load during work hours, no load over night), the cluster may become either underprovisioned (users experience bad performance) or overprovisioned (cluster is idle, causing a waste of resources and unnecessary costs).

However, while autoscaling has become state-of-the-art for applications in GCP, currently there exists no out-of-the-box solution for autoscalingof a Dataproc clusters.

The "Shamash" autoscaling tool actively monitors the performance of a Dataproc clusters and automatically scales the cluster up and down where appropriate. Shamash adds and remove nodes based on the current load of the cluster.

Shamash is build on top of Google App Engine utilizing a serveries architecture. 

### Highlights
* Serverless operation
* Support multiple clusters (each with his own configuration)
* Works without any change to the cluster


Flow

* Every x minutes cron job calls /tasks/check_load which calls /do_monitor
* For each cluster create a task that calls check_load()
* check_load() publish data to pub /sub             pubsub.publish(pubsub_client, msg, MONITORING_TOPIC)
* /get_monitoring_data in invoked and calls should_scale
* should_scale descide if we should scale. if yes trigger_scaling which put data into pubsub
* /scale invokes calls do_scale
* it call calc_scale which uses calc_slope
