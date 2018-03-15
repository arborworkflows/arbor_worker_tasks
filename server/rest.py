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

        file_id = str(file['_id'])
        item_id = str(file['itemId'])

        if file['mimeType'] != 'text/csv':
            raise RestException("File must be of type 'text/csv'", code=422)

        a = tasks.aggregateTableByAverage.delay(
            GirderFileId(file_id),
            column,
            girder_result_hooks=[GirderUploadToItem(item_id)]
        )

        return a.job
