from girder_worker.app import app
from girder_worker.utils import girder_job

@girder_job(title='Append Columns')
@app.task()
def appendColumns(inTable1File, inTable2File, indexColumn):
    # ------------------------------------------------------------------------
    # Input boilerplate
    # ------------------------------------------------------------------------
    from .io import fileToRows, rowsToFile
    inTable1 = fileToRows(inTable1File)
    inTable2 = fileToRows(inTable2File)

    # ------------------------------------------------------------------------
    # Task code (verbatim from Arbor task)
    # ------------------------------------------------------------------------
    # input: inTable1 - a list of rows (2D table)
    # input: inTable2 = a second list of rows
    # input: indexColumn - an attribute name to use as an index to merge data together
    # output: outTable

    # merge the columns together.  This analysis assumes that each table has an index column with the same
    # values in it, for example a "species" column with the same species names.  The species column would be used
    # as an index and the two character matrix tables would be combined into a single larger one with more columns.

    outTable = {}

    # prepare for the output table format of table:rows
    columnHeaders1 = inTable1['fields']
    columnHeaders2 = inTable2['fields']

    outRows = []
    outColumns = []

    # use the first table as the master.  Iterate through all of its rows and use the value of the index
    # column to select the proper row in the second table, so the additional attributes can be merged into the output
    for i in range(len(inTable1['rows'])):
        row = inTable1['rows'][i]
        for j in range(len(inTable2['rows'])):
            if inTable2['rows'][j][indexColumn] == row[indexColumn]:
                # this row from table2 matches, so loop through its entries and add them to the output row
                for k in inTable2['rows'][j]:
                    row[k] = inTable2['rows'][j][k]
                break;
        outRows.append(row)

    # now fix the column headers to be the union of both sets of input column headers

    for i in columnHeaders1:
        if i not in outColumns:
            outColumns.append(i)

    for i in columnHeaders2:
        if i not in outColumns:
            outColumns.append(i)

    outTable = {'fields': outColumns,'rows':outRows}

    # ------------------------------------------------------------------------
    # Output boilerplate
    # ------------------------------------------------------------------------
    outTableFile = rowsToFile(outTable)
    return outTableFile
