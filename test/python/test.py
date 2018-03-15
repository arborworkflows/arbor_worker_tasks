import arbor_worker_tasks.tasks as tasks


def testAppendColumns():
  f = tasks.appendColumns('in1.csv', 'in2.csv', 'id')

def testAggregateTableByAverage():
  f = tasks.aggregateTableByAverage('in3.csv', 'a')
