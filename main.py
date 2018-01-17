import logging

from flask import Flask, request

from monitoring import dataproc_monitoring
from scaling import scaling
from util import pubsub

app = Flask(__name__)


def create_app():
    logging.info("Starting {}".format("forseti"))
    client = pubsub.get_pubsub_client()
    pubsub.pull(client, 'monitoring',
                "https://aviv-playground.appspot.com/get_monitoring_data")
    pubsub.pull(client, 'scaling',
                "https://aviv-playground.appspot.com/scale")
    return app


create_app()


@app.route('/')
def hello():
    return 'OK', 200


@app.route('/get_monitoring_data', methods=['POST'])
def get_monitoring_data():
    return scaling.should_scale(request.json['message']['data'])


@app.route('/scale', methods=['POST'])
def scale():
    return scaling.do_scale(request.json['message']['data'])


@app.route('/test')
def test():
    return 'OK', 200


@app.route('/tasks/check_load')
def check_load():
    return dataproc_monitoring.check_load()


@app.errorhandler(500)
def server_error(e):
    # Log the error and stacktrace.
    logging.exception('An error occurred during a request.')
    return 'An internal error occurred.', 500


if __name__ == "__main__":
    app.run(debug=True)
