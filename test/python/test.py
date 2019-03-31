import arbor_worker_tasks.tasks as tasks
from arbor_worker_tasks.io import fileToRows

def testAppendColumns():
    out = tasks.appendColumns(fileToRows('in1.csv'), fileToRows('in2.csv'), 'id')
    print(out)

def testAggregateTableByAverage():
    out = tasks.aggregateTableByAverage(fileToRows('in3.csv'), 'a')
    print(out)

def testBuildTaxonMatrixFromEOL():
    out = tasks.buildTaxonMatrixFromEOLQuery('whale')
    print(out)
