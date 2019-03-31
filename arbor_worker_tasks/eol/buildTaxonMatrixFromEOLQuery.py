from girder_worker.app import app
from girder_worker.utils import girder_job

# for EOL interface tasks
import requests


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


