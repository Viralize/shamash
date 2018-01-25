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
