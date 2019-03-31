from girder_worker.app import app
from girder_worker.utils import girder_job

# for EOL interface tasks
import requests
#from lxml import html


@girder_job(title='Append Columns')
@app.task()
def appendColumns(inTable1, inTable2, indexColumn):
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
                break
        outRows.append(row)

    # now fix the column headers to be the union of both sets of input column headers

    for i in columnHeaders1:
        if i not in outColumns:
            outColumns.append(i)

    for i in columnHeaders2:
        if i not in outColumns:
            outColumns.append(i)

    outTable = {'fields': outColumns,'rows':outRows}

    return outTable


@girder_job(title='Aggregate Table by Average')
@app.task()
def aggregateTableByAverage(table, column):
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



@girder_job(title='Build Taxon Matrix from EOL Query')
@app.task()
def buildTaxonMatrixFromEOLQuery(query,maxImagesPerTaxon=5):
   
    #
    # access the EOL API to find matching taxa/subtaxa that match a query term
    # input: query (string)
    # outputs: matrix - a single entry for each species, along with its EOL link and an image
    # imageLinks: a two column table, with a row for every image discovered.  The schema is 'name','url'
    # maxImagesPerTaxon: an integer specifying how many images to return for a single species

    # prerequisites: lxml, requests, string

    eolquery = 'http://eol.org/api/search/1.0.json?q='+query
    response = requests.get(eolquery)
    jsonReturn = response.json()

    # now go through the results returned and pull out the name and EOL page number for each 
    # search return (taxon) which matched.

    # start with an empty output matrix
    matrix = {}
    matrix['fields'] = ['name','pagenumber','page','image']
    matrix['rows'] = []

    # create an empty image url table
    # *** this output is currenely not supported in worker tasks   
    imageLinks = {}
    imageLinks['fields'] = ['name','url']
    imageLinks['rows'] = []

    # get rid of any words past the first two and suppress following punctuation, so names like 'Carnivora Bowdich, 1821'
    # will be output as 'Carnivora bowdich'.  Capitalization is enforced for first word and lowercase on second word,
    # since this is the Latin name (scientific name) convention
    def cleanTaxonName(name):
        spaceSplit = name.split(' ')
        if len(spaceSplit)>1:
            clean = spaceSplit[0].capitalize() + ' '+ spaceSplit[1].lower()
        else:
            clean = spaceSplit[0].capitalize()
        return clean.replace(',','')

    # some duplicates return from the search, so put the names in a set and check
    # that only unique entries are added to the table
    nameSet = set()

    for res in jsonReturn['results']:
        name = res['title']
        link = res['link']
        print('***Exploring name:',name)
        
        # many informal taxonomies with creator names , pass only the first two words in the taxonomic name
        cleanedName = cleanTaxonName(name)
        #print('name cleaned to:',cleanedName)
        
        # test if we have seen this taxon before
        if cleanedName not in nameSet:
            nameSet.add(cleanedName)

            # store in output row; the cleaning operation is largely redundant, but this same routine is used in the 
            # "tree from matrix" method, so we want to make sure the matrix names and tip names match as much as practical
            taxaRow = {}
            taxaRow['name'] =  cleanedName
        
            #find the page number and images for this taxon.  We extract the page number from the page link
            questionMarkPosition = link.find('?')
            eolPageNumber = link[15:questionMarkPosition]
            #print('page number:',eolPageNumber)
            taxaRow['pagenumber'] = eolPageNumber
            taxaRow['page'] = link
        
            # now find the images on the corresponding taxon media page
            mediaQuery = 'https://eol.org/api/'+eolPageNumber+'.json?details=true&images_per_page=10'
            #print('mediaQuery:',mediaQuery)
            response = requests.get(mediaQuery)
            try:
                jsonReturn = response.json()
                images = []
                if 'dataObjects' in jsonReturn['taxonConcept']:
                    dataObjects = jsonReturn['taxonConcept']['dataObjects']
                    for obj in dataObjects:
                        if 'mediaURL' in obj:
                            images.append(obj['mediaURL'])
            except:
               print('passing over exception page:',eolPageNumber)

            # use sets to remove duplicates.  This assumes the names and images duplicate at the same time. This could
            # be a risky assumption. 
            uniqueImages = set(images)                     
            
            # return only the first five in the list. Use Max function to 
            # Return fewer images if there were less than the specified max available
            for index in range(min(len(uniqueImages),maxImagesPerTaxon)):
                imageRow = {}
                # have to extract from sets, not use an index
                url = uniqueImages.pop()
                imageRow['name'] = taxaRow['name']
                imageRow['url'] = url
                imageLinks['rows'].append(imageRow)
                if index == 0:
                    taxaRow['image'] = url
            # add this taxa to the output table        
            matrix['rows'].append(taxaRow)
    
    return matrix


