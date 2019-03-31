from girder_worker.app import app
from girder_worker.utils import girder_job


@girder_job(title='Aggregate Table by Average')
@app.task()
def AggregateTableByAverage(table, column):
    # roll up the values of a table's rows according to discrete values in a selected "groupBy" column.
    # The number of output rows in the table will be equal to the number of discrete values in the
    # groupBy column.  The values in the other columns will be the average of the values of all rows which
    # belonged to this group from the input table.   Therefore, this is a simple aggregation by one column.

    # input: table - input table of continuous values
    # input: column - column name used as the "group by" control
    # output: output - output table


    # helper function to check for numberic values only
    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False


    # loop through the control column and build a list of the discrete values it contains
    valuelist = []
    for row in table['rows']:
        if row[column] not in valuelist:
            valuelist.append(row[column])

    outputrows = []

    # now loop through the entire table once for each "class" and sum up the values for each attribute in order
    # to calculate the average value for each continuous variable for each class by accumulating the values for each
    # attribute and dividing by the number of entires in each class.

    for classValue in valuelist:
        sumobject = {}
        classMemberCount = 0
        # initialize an empty summation object
        for field in table['fields']:
            sumobject[field] = 0.0
        # now loop through this class's entries and accumulate values
        for row in table['rows']:
            if (row[column] == classValue):
                # this row is a member of the current class, so add its contribution and update the count of class members
                    for attrib in row:
                        # if this is a continuous variable, accumulate.  If it is not numberic, just copy it across
                        if is_number(row[attrib]):
                            # make sure the previous accumulations are also numbers
                            if is_number(sumobject[attrib]):
                                sumobject[attrib] += float(row[attrib])
                        else:
                            #if this is non-numeric, just copy it, except: ignore 'NA' and ignore empty cells:
                            if (row[attrib] != "NA")  and (len(row[attrib]) != 0):
                                sumobject[attrib] = row[attrib]
                    classMemberCount += 1
        # create the output row corresponding to this class by dividing the accumulated sums by the number of members
        # in each class
        outrow = {}
        for field in sumobject:
            if is_number(sumobject[field]):
                outrow[field] = sumobject[field]/classMemberCount
            else:
                # if this field is a non-numeric value, just copy it across instead of averaging
                outrow[field] = sumobject[field]
        outputrows.append(outrow)

    # create an output table in the table:rows format by creating a list of the fields and a list of the table rows
    output = {}
    output['fields'] = table['fields']
    output['rows'] = outputrows

    return output

