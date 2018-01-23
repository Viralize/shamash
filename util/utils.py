"""Misc utils"""
import json
import logging
import os

from google.appengine.api import app_identity


def detect_gae():
    """Determine whether or not we're running on GAE.

    This is based on:
      https://developers.google.com/appengine/docs/python/#The_Environment

    Returns:
      True iff we're running on GAE.
    """
    server_software = os.environ.get('SERVER_SOFTWARE', '')
    return not (server_software.startswith('Development/'))


def _get_project_id():
    logging.info("-------------------Running Localy--------------------")
    with open('config.json', 'r') as f:
        config = json.load(f)
    return config['project']


def get_project_id():
    if detect_gae():
        project = app_identity.get_application_id()
    else:
        project = _get_project_id()
    return project


def get_host_name():
    if detect_gae():
        hostname = "{}.appspot.com".format(app_identity.get_application_id())
    else:
        hostname = "{}.appspot.com".format(_get_project_id())
    return hostname


def fatal_code(e):
    return e.resp.status < 500
