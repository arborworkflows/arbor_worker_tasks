from girder.api import access
from girder.api.describe import Description, autoDescribeRoute
from girder.api.rest import Resource, RestException, filtermodel
from girder.constants import AccessType, TokenScope
from girder.models.file import File as FileModel
from girder.plugins.jobs.models.job import Job as JobModel # pylint: disable=E0611,E0401

from girder_worker_utils.transforms.girder_io import (
    GirderFileId,
    GirderItemMetadata,
    GirderUploadToItem)

from arbor_worker_tasks import tasks
from arbor_worker_tasks.transforms import GirderFileToRows, RowsToGirderItem


class ArborTasks(Resource):

    def __init__(self):
        super(ArborTasks, self).__init__()
        self.resourceName = 'arbor_tasks'
        self.route('POST', ('aggregate_table_by_average', ':id', ':column'), self.aggregateTableByAverage)

    @access.public(scope=TokenScope.DATA_READ) # pylint: disable=E1120
    @filtermodel(model=JobModel)
    @autoDescribeRoute(
        Description('Aggregate table by a column, averaging other column values.')
        .responseClass('Job')
        .modelParam('id', model=FileModel, level=AccessType.READ)
        .param('column', description='The column to aggregate by.')
        .errorResponse('ID was invalid.')
        .errorResponse('Read access was denied for the item.', 403)
    )
    def aggregateTableByAverage(self, file, column):
        fileId = str(file['_id'])
        itemId = str(file['itemId'])

        if file['mimeType'] != 'text/csv':
            raise RestException("File must be of type 'text/csv'", code=422)

        a = tasks.aggregateTableByAverage.delay(
            GirderFileToRows(fileId),
            column,
            girder_result_hooks=[RowsToGirderItem(itemId, 'aggregated.csv')]
        )

        return a.job

  @access.public(scope=TokenScope.DATA_READ) # pylint: disable=E1120
    @filtermodel(model=JobModel)
    @autoDescribeRoute(
        Description('Accept a string of an organism to explore.  Return a matrix of possible taxa from Encyclopia of Life.')
        .responseClass('Job')
        # what do we use for modelParams?
        .modelParam('string')
        .param('query', description='The taxa name search string')
        .errorResponse('Please enter a string')
    )
    def buildTaxonMatrixFromEOLQuery(self, query):

        a = tasks.buildTaxonMatrixFromEOLQuery.delay(
            query,
            girder_result_hooks=[RowsToGirderItem(itemId, 'taxa_matrix.csv')]
        )

        return a.job

