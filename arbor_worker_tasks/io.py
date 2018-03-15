
def fileToRows(filename):
    import pandas as pd

    df = pd.read_csv(filename)
    return {
        'fields': df.columns,
        'rows': [{k: v for k, v in row.iteritems()} for _, row in df.iterrows()]
    }

def rowsToFile(table, outFile='out.csv'):
    import pandas as pd

    df = pd.DataFrame(table['rows'], columns=table['fields'])
    df.to_csv(outFile, index=False)
    return outFile
