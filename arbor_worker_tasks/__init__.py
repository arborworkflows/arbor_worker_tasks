# -*- coding: utf-8 -*-

"""Top-level package for arbor_worker_tasks."""

__author__ = """Kitware Inc"""
__email__ = 'kitware@kitware.com'
__version__ = '0.0.0'


from girder_worker import GirderWorkerPluginABC


class ArborWorkerTasks(GirderWorkerPluginABC):
    def __init__(self, app, *args, **kwargs):
        self.app = app

    def task_imports(self):
        # Return a list of python importable paths to the
        # plugin's path directory
        return ['arbor_worker_tasks.tasks']
