from .rest import ArborTasks

def load(info):
    info['apiRoot'].arbor_tasks = ArborTasks()
