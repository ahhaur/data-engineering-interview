"""
Implement a structured data flow orchestration to process the raw data to generate 
a set of pipe-delimited load files and stored in the path (/dataextract/yy/MM/dd/factstore).
"""

import os
import pandas as pd

def get_insert_str (_tblname, _cols, _vals):
    ret = "INSERT " + _tblname
    ret += " (["+"], [".join(_cols)+"]) VALUES"
    ret += " ("+", ".join([str(_) for _ in _vals])+")"
    return ret

# define
dbCols = ['Date','StoreID','ProductID','OnHandQty','OnOrderQty','DaysInStock','MinDayInStock','MaxDayInStock']
dataCols = ['Date','StoreID','ProductID','OnHandQuantity','OnOrderQuantity','DaysInStock','MinDayInStock','MaxDayInStock']
tablename = "[dbo].[FactStore]"

# set folder
rawFolder = os.path.join('.', 'RawData', '2009')
extractFolder = os.path.join('.', 'dataextract')

# scan year folder
for mthFolder in os.listdir(rawFolder):
    mthPath = os.path.join(rawFolder, mthFolder)
    #scan month folder
    for dayFolder in os.listdir(mthPath):
        dayPath = os.path.join(mthPath, dayFolder)
        # target folder
        targetPath = os.path.join(extractFolder, '2009', mthFolder, dayFolder)
        # target filename
        targetFile = os.path.join(targetPath, 'factstore')

        # check folder exist
        if not os.path.exists(targetPath):
            os.makedirs(targetPath)
        
        # write into file
        with open(targetFile, 'a') as f:
            # scan file by file
            for file in os.listdir(dayPath):
                datafile = os.path.join(dayPath, file)

                # read file
                print ("Reading %s.." % datafile)
                df = pd.read_csv(datafile, compression='gzip', header=0, sep="\t")
                df = pd.DataFrame(df, columns=dataCols)

                # write line by line
                for index, row in df.iterrows():
                    # handle date
                    row['Date'] = "CONVERT(datetime, N'"+row['Date']+" 00:00:00', 120)"
                    # get insert line
                    sql = get_insert_str(tablename, dbCols, row)
                    # write file
                    f.write(sql+"\n"+"GO"+"\n")

print ("Done !")
