"""Entry point for Shmaash"""
import logging

from flask import Flask, request
from google.appengine.api import app_identity

from monitoring import dataproc_monitoring
from scaling import scaling
from util import pubsub

app = Flask(__name__)


def create_app():
    """
    Do initialization
    """
    hostname = app_identity.get_default_version_hostname()
    logging.info("Starting {} on {}".format("Shamash", hostname))
    client = pubsub.get_pubsub_client()
    pubsub.pull(client, 'monitoring',
                "https://{}/get_monitoring_data".format(hostname))
    pubsub.pull(client, 'scaling',
                "https://{}/scale".format(hostname))


create_app()


@app.route('/')
def hello():
    """
    Main Page
    :return:
    """
    return 'OK', 200


@app.route('/get_monitoring_data', methods=['POST'])
def get_monitoring_data():
    """
    After data is gathered from cluster into pub sub this function is invoked
    :return:
    """
    return scaling.should_scale(request.json['message']['data'])


@app.route('/scale', methods=['POST'])
def scale():
    """
    Called whenwe decide  to scale is made
    :return:
    """
    return scaling.do_scale(request.json['message']['data'])


@app.route('/tasks/check_load')
def check_load():
    """Entry point for cron task that check cluster stats"""
    return dataproc_monitoring.check_load()


@app.errorhandler(500)
def server_error(e):
    """Log the error and stacktrace."""
    logging.exception('An error occurred during a request. {}'.format(e))
    return 'An internal error occurred.', 500


if __name__ == "__main__":
    app.run(debug=True)
