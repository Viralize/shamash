#!/usr/bin/env bash
# TODO craete pubsub topics and subscriptions in code
if [ $# -eq 0 ]
  then
    error_exit "No arguments supplied"
fi

PROJECTID=`gcloud projects list | grep -i $1 |  awk '{print $1}'`
APP=https://$PROJECTID.appspot.com/
if [ -z "$PROJECTID" ]; then
 echo Project $1 Not Found!
 exit
fi
echo Project ID $PROJECTID App is $APP
gcloud config set project $PROJECTID

echo crateing topics
gcloud beta pubsub topics create shamash-monitoring --project=$PROJECTID
gcloud beta pubsub topics create shamash-scaling --project=$PROJECTID

gcloud beta pubsub subscriptions create monitoring	--topic=shamash-monitoring  --topic-project=$PROJECTID --push-endpoint $APP/get_monitoring_data
gcloud beta pubsub subscriptions create scaling	--topic=shamash-scaling --topic-project=$PROJECTID --push-endpoint $APP/scale


gcloud app deploy app.yaml cron.yaml queue.yaml