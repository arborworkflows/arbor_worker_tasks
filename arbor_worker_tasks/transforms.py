from girder_worker_utils.transforms.girder_io import GirderClientTransform, GirderClientResultTransform
import os
import shutil
import tempfile
from .io import fileToRows, rowsToFile


class GirderFileToRows(GirderClientTransform):
    def __init__(self, _id, **kwargs):
        super(GirderFileToRows, self).__init__(**kwargs)
        self.fileId = _id

    def transform(self):
        self.filePath = os.path.join(
            tempfile.mkdtemp(), '{}'.format(self.fileId))

        self.gc.downloadFile(self.fileId, self.filePath)

        return fileToRows(self.filePath)

    def cleanup(self):
        shutil.rmtree(os.path.dirname(self.filePath), ignore_errors=True)


class RowsToGirderItem(GirderClientResultTransform):
    def __init__(self, _id, fileName=None, **kwargs):
        super(RowsToGirderItem, self).__init__(**kwargs)
        self.itemId = _id
        self.fileName = fileName

    def transform(self, data):
        self.filePath = os.path.join(
            tempfile.mkdtemp(),
            self.fileName if self.fileName else '{}.csv'.format(self.itemId)
        )

        rowsToFile(data, self.filePath)

        self.gc.uploadFileToItem(self.itemId, self.filePath)
        return self.itemId

    def cleanup(self):
        shutil.rmtree(os.path.dirname(self.filePath), ignore_errors=True)
