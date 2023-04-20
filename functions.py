"""
UNIVERSIDAD DEL VALLE DE GUATEMALA
BASES DE DATOS 2

PROYECTO 2: 
HDFS SIMULATOR

INTEGRANTES:
JUN WOO LEE, 20979
ANDRES DE LA ROCA, 20979
ROBERTO RIOS, 20979

ESTE PROGRAMA CONTIENE LA IMPLEMENTACION DE LAS FUNCIONES QUE HACEN LLAMADAS A UNA BASE DE DATOS LOCAL,
IMPLEMENTA LO SOLICITADO EN LAS INSTRUCCIONES DEL PROYECTO DE HDFS
"""

import os
import pandas as pd
import time
import warnings

current_dir = os.getcwd()
subfolder_name = 'DataBase'
DataBasePath = os.path.join(current_dir, subfolder_name)

#region lenguaje de definicion de datos

def create(command): 
    startTime = time.time() #Start timer
    fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
    disabledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled

    if not os.path.exists(fileName): #If file doesn't exist create
        if not os.path.exists(disabledFileName): #If disabled file doesn't exist create
            cols = command[1:] #Get column names, everything except first value
            if len(cols) == 0: #No columns specified throw error
                cols.append("Default")

            cols.insert(0, "RowId")
            cols.insert(len(cols),"timestamp")
            #Set Default column properties
            properties = ["DATA_BLOCK_ENCODING='NONE'", "BLOOMFILTER='ROW'", 
                          "REPLICATION_SCOPE='0'", "COMPRESSION='NONE'", "TTL='FOREVER'",
                          "KEEP_DELETED_CELLS='FALSE'", "BLOCKSIZE='65536'",
                          "IN_MEMORY='FALSE'", "BLOCKCACHE='FALSE'"]
            data = {col: [] for col in cols}
            for col in cols:
               data[col] = properties
            
            
            #Create CSV
            df = pd.DataFrame(data, columns=cols)
            df.to_csv(fileName, index=False)

            endTime = time.time() #End Timer
            elapsedTime = endTime - startTime #Get Run time
            print("Created table ", command[0])
            print(f"0 row(s) in  {elapsedTime:.5f} seconds")
        else: #If disabled file exists, print error file already exists
            print("Error: Table employees already exists")
    else: #File already exists so just print error
        print("Error: Table employees already exists")


def hlist():
    startTime = time.time() #Start timer
    files = os.listdir(DataBasePath)
    fileNames = [os.path.splitext(file)[0] for file in files]
    for table in fileNames:
        if "_Disabled" in table:
            table = table.replace("_Disabled", "")
            # print(table, "DISABLED")
            print("{:<20} {}".format(table, "DISABLED"))
        else:
            print(table)

    endTime = time.time() #End Timer
    elapsedTime = endTime - startTime #Get Run time
    print(len(fileNames), f"row(s) in  {elapsedTime:.5f} seconds")


def disable(command):
    startTime = time.time() #Start timer
    fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
    disabledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled

    if not os.path.exists(disabledFileName): #If the disabled table doesnt exist
        if not os.path.exists(fileName): #If file doesn't exist throw error
            print("ERROR: Table ", command[0], " not found")
        else: #File exists, so disable (Disable by renaming)
            newFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Rename the file adding _Disabled to the end of it to emulate it being disabled
            os.rename(fileName ,newFileName)
            endTime = time.time() #End Timer
            elapsedTime = endTime - startTime #Get Run time
            print(f"0 row(s) in  {elapsedTime:.5f} seconds")
        
    else: #If the disabled table exists
        print("Error: Table ", command[0], " is already disabled")

    
def isEnabled(command):
    fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
    disabledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled

    if not os.path.exists(fileName) and not os.path.exists(disabledFileName): #If both enabled and Disabled files dont exist throw error
        print("ERROR: Table ", command[0], " not found")
    else: #If one of them exists check which one
        if os.path.exists(fileName): #Enabled table exists, so return true
            print("true")
            return True
        elif os.path.exists(disabledFileName): #Disabled table exists, so return false
            print("false")
            return False



def alter(command):
    '''
    Implement ways to do:
    New Column Family, the 2 different ways 
    Delete Column family
    Modify Column Family properties
    Rename coolumn Family
    '''
    fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
    disabledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled

    if not os.path.exists(fileName) and not os.path.exists(disabledFileName): #If both enabled and Disabled files dont exist throw error
        print("ERROR: Table ", command[0], " not found")
    else: #If one of them exists check which one
        for table in os.listdir(DataBasePath):
            if "_Disabled.csv" in table:
                
                filePath = os.path.join(DataBasePath, table)
                os.remove(filePath)
            else:
                table = table.replace(".csv", "")


def drop(command):
    fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
    disabledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled

    if not os.path.exists(fileName) and not os.path.exists(disabledFileName): #If both enabled and Disabled files dont exist throw error
        print("ERROR: Table ", command[0], " not found")
    else: #If one of them exists check which one
        if os.path.exists(fileName): #Enabled table exists, so return error that it needs to be disabled first
            print("ERROR: Table ", command[0], " should be disabled before dropping")
        elif os.path.exists(disabledFileName): #Disabled table exists, so delete the file
            os.remove(disabledFileName)
            print()


def dropAll(): #Idk if need to implement with regex argument, Did it without for now
    startTime = time.time() #Start timer
    notDropped = []
    for table in os.listdir(DataBasePath):
        if "_Disabled.csv" in table:
            
            filePath = os.path.join(DataBasePath, table)
            os.remove(filePath)
        else:
            table = table.replace(".csv", "")
            notDropped.append(table)
    endTime = time.time() #End Timer
    elapsedTime = endTime - startTime #Get Run time
    print(f"0 row(s) in  {elapsedTime:.5f} seconds")
    if(len(notDropped) != 0): #If size of notDropped array is != 0, then print which ones where not dropped
        print("Following tables could not be dropped becasue they are not disabled: " + ", ".join(notDropped) + ".")
        print("Please disable these tables first before running drop_all command again")

def describe(command):
    startTime = time.time() #Start timer
    fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
    disabledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled

    if os.path.exists(fileName): #If file exists get data
        df = pd.read_csv(fileName)
        print("Table", command[0], "is ENABLED\n" + command[0] + "\nCOLUMN FAMILIES DESCRIPTION")
        notUsedColumn = 0
        for column in df.columns:
            if column == 'RowId' or column == 'timestamp':
                notUsedColumn += 1
            else:
                values = df[column]
                newValues = {}
                for i, string in enumerate(values):
                    if i >= 9:
                        break
                    key, value = string.split('=')
                    newValues[key] = value
                print("{NAME => '", column, "',", end=' ')
                print("DATA_BLOCK_ENCODING =>", newValues["DATA_BLOCK_ENCODING"], ",", end=' ')
                print("BLOOMFILTER =>", newValues["BLOOMFILTER"], ",", end=' ')
                print("REPLICATION_SCOPE =>", newValues["REPLICATION_SCOPE"], ",", end=' ')
                print("COMPRESSION =>", newValues["COMPRESSION"], ",", end=' ')
                print("TTL =>", newValues["TTL"], ",", end=' ')
                print("KEEP_DELETED_CELLS =>", newValues["KEEP_DELETED_CELLS"], ",", end=' ')
                print("BLOCKSIZE =>", newValues["BLOCKSIZE"], ",", end=' ')
                print("IN_MEMORY =>", newValues["IN_MEMORY"], ",", end=' ')
                print("BLOCKCACHE =>", newValues["BLOCKCACHE"])

        endTime = time.time() #End Timer
        elapsedTime = endTime - startTime #Get Run time
        print(len(df.columns) - notUsedColumn, f"row(s) in  {elapsedTime:.5f} seconds")

    elif os.path.exists(disabledFileName): #Disbaled file exists, throw error
        print("ERROR: Table ", command[0], " is disabled")
        print("Cannot describe a disabled table")
    elif not os.path.exists(fileName) and not os.path.exists(disabledFileName): #If both enabled and Disabled files dont exist throw error
        print("ERROR: Table ", command[0], " not found")
    
#endregion
    
#region lenguaje de manipulacion de datos

def put(command):
    warnings.filterwarnings("ignore", message="The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.")
    startTime = time.time() #Start timer
    fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
    disabledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled

    if os.path.exists(fileName): #If file exists get data
        df = pd.read_csv(fileName)
        #Split the column family and column into another array
        commandColumn = command[2].split(":")
        #If Inputted RowId doesnt exist then create new Row
        if command[1] not in df["RowId"].values: 
            newRow = {'RowId': command[1]}
            if commandColumn[0] in df.columns:
                newRow[commandColumn[0]] = str(commandColumn[1]) + ":" + str(command[3])
                timestamp = int(time.time() * 1000)
                hbase_timestamp = (2**64 - timestamp) ^ (2**63)
                newRow['timestamp'] = hbase_timestamp
                df = df.append(newRow, ignore_index=True)
                df.to_csv(fileName, index=False)
                endTime = time.time() #End Timer
                elapsedTime = endTime - startTime #Get Run time
                print(f"1 row(s) in  {elapsedTime:.5f} seconds")

        #If Inputted RowId exists, then edit that Row
        else:
            # Filter the rows based on the RowId and additional criteria
            mask = (df['RowId'] == command[1]) & (df[commandColumn[0]].str.contains(commandColumn[1]))
            rows = df.loc[mask]

            if len(rows) == 0:
                # If the correct row is not found, add a new row with the given RowId
                newRow = {'RowId': command[1]}
                if commandColumn[0] in df.columns:
                    newRow[commandColumn[0]] = str(commandColumn[1]) + ":" + str(command[3])
                
                timestamp = int(time.time() * 1000)
                hbase_timestamp = (2**64 - timestamp) ^ (2**63)
                newRow['timestamp'] = hbase_timestamp
                df = df.append(newRow, ignore_index=True)
                df.to_csv(fileName, index=False)
                endTime = time.time() #End Timer
                elapsedTime = endTime - startTime #Get Run time
                print(f"1 row(s) in  {elapsedTime:.5f} seconds")

            else:
                # Edit the first row that matches the criteria
                rowIndex = rows.index[0]
                df.at[rowIndex, commandColumn[0]] = str(commandColumn[1]) + ":" + str(command[3])
                timestamp = int(time.time() * 1000)
                hbase_timestamp = (2**64 - timestamp) ^ (2**63)
                df.at[rowIndex, 'timestamp'] = hbase_timestamp
                df.to_csv(fileName, index=False)
                endTime = time.time() #End Timer
                elapsedTime = endTime - startTime #Get Run time
                print(f"1 row(s) in  {elapsedTime:.5f} seconds")

    elif os.path.exists(disabledFileName): #Disbaled file exists, throw error
        print("ERROR: Table ", command[0], " is disabled")
        print("Cannot describe a disabled table")
    elif not os.path.exists(fileName) and not os.path.exists(disabledFileName): #If both enabled and Disabled files dont exist throw error
        print("ERROR: Table ", command[0], " not found")

def get(command):
    warnings.filterwarnings("ignore", message="The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.")
    startTime = time.time() #Start timer
    fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
    disabledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled
    if os.path.exists(fileName): # If file exists get data
        df = pd.read_csv(fileName)

        if command[1] in df["RowId"].values: # If RowId exists continues
            selected_rows = df.loc[df["RowId"] == command[1]] # Selects the individual rows
            row_counter = len(selected_rows)

            row_data = []
            timestamp_data = []
            for row_index in range(row_counter): # Separates the data of the row (basically cleaning the data)
                row = selected_rows.iloc[row_index]
                row = row.dropna()
                row_data.append(row[1].split(':'))
                timestamp_data.append(row[2])
            
            RowId = row[0]
            result_df = pd.DataFrame(columns=['COLUMN', 'CELL'])

            timestamp_index = 0
            for rows in row_data: # Sets the correct format for the data to print into
                new_row = {'COLUMN':f'{RowId}:{rows[0]}', "CELL":f'timestamp={timestamp_data[timestamp_index]}, value={rows[1]}'}
                result_df = result_df.append(new_row, ignore_index=True)
                timestamp_index += 1

            print(result_df.to_string(index=False))
            endTime = time.time() # End timer
            elapsedTime = endTime - startTime # Get run time
            print(f"{row_counter} row(s) in {elapsedTime:.5f} seconds")
            
        else: # In case row is not found
            endTime = time.time()
            elapsedTime = endTime - startTime
            print(f'0 row(s) in {elapsedTime:.5f} seconds')

    elif os.path.exists(disabledFileName): #Disabled file exists, throw error
        print("ERROR: Table ", command[0], " is disabled")
        print("Cannot describe a disabled table")
    elif not os.path.exists(fileName) and not os.path.exists(disabledFileName): #If both enabled and Disabled files dont exist throw error
        print("ERROR: Table ", command[0], " not found")
    


def scan(command):
    warnings.filterwarnings("ignore", message="The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.")
    startTime = time.time() #Start timer
    fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
    disabledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled
    if os.path.exists(fileName): # If file exists get data
        df = pd.read_csv(fileName)
        df = df.drop(df.index[0:4]) # Drops the column metadata
        row_counter = df['RowId'].nunique() # Counts the amount of rows there is (Only accounting for the row Id)


        row_data = []
        for row_index in range(len(df)):
            row = df.iloc[row_index]
            row = row.dropna()
            row_data.append(row)

        result_df = pd.DataFrame(columns=["ROW", "COLUMN+CELL"])

        for row in row_data:
            row_id = row[0]
            value_data = row[1].split(':')
            timestamp = row[2]
            new_row = {'ROW': row_id, 'COLUMN+CELL': f'column={row.index[1]}:{value_data[0]}, timestamp={timestamp}, value={value_data[1]}'}
            result_df = result_df.append(new_row, ignore_index=True)
        
        print(result_df.to_string(index=False))
        endTime = time.time() # End timer
        elapsedTime = endTime - startTime # Get run time
        print(f"{row_counter} row(s) in {elapsedTime:.5f} seconds")

    elif os.path.exists(disabledFileName): #Disabled file exists, throw error
        print("ERROR: Table ", command[0], " is disabled")
        print("Cannot describe a disabled table")
    elif not os.path.exists(fileName) and not os.path.exists(disabledFileName): #If both enabled and Disabled files dont exist throw error
        print("ERROR: Table ", command[0], " not found")


def delete(command):
    warnings.filterwarnings("ignore", message="The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.")
    fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
    disabledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled
    if os.path.exists(fileName): # If file exists get data
        df = pd.read_csv(fileName)

        if command[1] in df["RowId"].values:

            if len(command) > 2: # In case the column_family:column parameter is found, the specified column data will be deleted
                column_data = command[2].split(':')
                column_family_name = column_data[0]
                column_name = column_data[1]

                row_to_delete = (df['RowId'] == command[1]) & (df[column_family_name].str.contains(column_name))

                df = df.drop(df[row_to_delete].index)
                df.to_csv(fileName, index=False)


            else: # In case only the row parameter is found, the whole row will be deleted
                df = df.drop(df[df.apply(lambda x: x.str.contains(command[1])).any(axis=1)].index)
                df.to_csv(fileName, index=False)

        else: # In case row is not found
            print("ERROR: Cannot delete row from a non-existent row key")

    elif os.path.exists(disabledFileName): #Disabled file exists, throw error
        print("ERROR: Table ", command[0], " is disabled")
        print("Cannot describe a disabled table")
    elif not os.path.exists(fileName) and not os.path.exists(disabledFileName): #If both enabled and Disabled files dont exist throw error
        print("ERROR: Table ", command[0], " not found")

def deleteAll(command):
    warnings.filterwarnings("ignore", message="The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.")
    startTime = time.time() #Start timer
    fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
    disabledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled
    if os.path.exists(fileName): # If file exists get data
        df = pd.read_csv(fileName)

        if command[1] in df["RowId"].values:

            if len(command) > 2: # In case the endrow parameter is found, specified range of rows will be deleted
                startrow = command[1]
                endrow = command[2]

                df = df.loc[~((df['RowId'] >= startrow) & (df['RowId'] <= endrow))]

                df.to_csv(fileName, index=False)

            else: # In case only the row parameter is found, the whole row will be deleted
                df = df.drop(df[df.apply(lambda x: x.str.contains(command[1])).any(axis=1)].index)
                df.to_csv(fileName, index=False)

        else: # In case row is not found
            print("ERROR: Cannot delete row from a non-existent row key")



    elif os.path.exists(disabledFileName): #Disabled file exists, throw error
        print("ERROR: Table ", command[0], " is disabled")
        print("Cannot describe a disabled table")
    elif not os.path.exists(fileName) and not os.path.exists(disabledFileName): #If both enabled and Disabled files dont exist throw error
        print("ERROR: Table ", command[0], " not found")

def count(command):
    warnings.filterwarnings("ignore", message="The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.")
    startTime = time.time() #Start timer
    fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
    disabledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled
    if os.path.exists(fileName): # If file exists get data
        df = pd.read_csv(fileName)
        df = df.drop(df.index[0:9]) # Drops the column metadata

        row_counter = df['RowId'].nunique() # Counts the amount of rows (Only accounting for unique rowids inside the csv)
        print(f"COUNT\n{row_counter}")


def truncate(command: list[str]):
    # check if the length to be truncated was given
    t = command[0]
    if not t.isdigit():
        print('Warning: You need to specify the length to truncate, \nexample: truncate 55,tablename,tablename2')
    else:
        print(f' truncating after: {t}')
        for i in command[1:]:
            # check if file exists
            fileName = os.path.join(DataBasePath, i + '.csv') # Get the file path and file name
            print(f'\nmodifying: {i}')
            t = int(t) - 1
            
            if os.path.isfile(fileName):
                # save metadata
                df = pd.read_csv(fileName)
                dfmeta = df[df['RowId'].str.contains('=')]
                print(f'\n metadata:\n{dfmeta}')
                # adjust seek
                print(f'\n metadata shape:\n{dfmeta.shape}')
                dfcontent = df[~df['RowId'].str.contains('=')]
                dfcontent = dfcontent.reset_index(drop=True)
                print(f'\n before:\n{dfcontent}')
                print(f'\nbefore truncate:{dfcontent.shape}')
                dfcontent = dfcontent.truncate(after=t)
                print(f'\nafter:\n{dfcontent}')
                print(f'\nafter truncate:{dfcontent.shape}')

                # resultado de truncar
                result = pd.concat([dfmeta, dfcontent])
                result.to_csv(fileName, index=False)

            else:
                print(f'Warning: file {i} not in folder')


#endregion

#region puntos extra

def updateMany(command):
    for i in command:
        warnings.filterwarnings("ignore", message="The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.")
        startTime = time.time() #Start timer
        fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
        disabledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled

        if os.path.exists(fileName): #If file exists get data
            df = pd.read_csv(fileName)
            #Split the column family and column into another array
            commandColumn = command[2].split(":")
            #If Inputted RowId doesnt exist then create new Row
            if command[1] not in df["RowId"].values: 
                newRow = {'RowId': command[1]}
                if commandColumn[0] in df.columns:
                    newRow[commandColumn[0]] = str(commandColumn[1]) + ":" + str(command[3])
                    timestamp = int(time.time() * 1000)
                    hbase_timestamp = (2**64 - timestamp) ^ (2**63)
                    newRow['timestamp'] = hbase_timestamp
                    df = df.append(newRow, ignore_index=True)
                    df.to_csv(fileName, index=False)
                    endTime = time.time() #End Timer
                    elapsedTime = endTime - startTime #Get Run time
                    print(f"1 row(s) in  {elapsedTime:.5f} seconds")

            #If Inputted RowId exists, then edit that Row
            else:
                # Filter the rows based on the RowId and additional criteria
                mask = (df['RowId'] == command[1]) & (df[commandColumn[0]].str.contains(commandColumn[1]))
                rows = df.loc[mask]

                if len(rows) == 0:
                    # If the correct row is not found, add a new row with the given RowId
                    newRow = {'RowId': command[1]}
                    if commandColumn[0] in df.columns:
                        newRow[commandColumn[0]] = str(commandColumn[1]) + ":" + str(command[3])
                    
                    timestamp = int(time.time() * 1000)
                    hbase_timestamp = (2**64 - timestamp) ^ (2**63)
                    newRow['timestamp'] = hbase_timestamp
                    df = df.append(newRow, ignore_index=True)
                    df.to_csv(fileName, index=False)
                    endTime = time.time() #End Timer
                    elapsedTime = endTime - startTime #Get Run time
                    print(f"1 row(s) in  {elapsedTime:.5f} seconds")

                else:
                    # Edit the first row that matches the criteria
                    rowIndex = rows.index[0]
                    df.at[rowIndex, commandColumn[0]] = str(commandColumn[1]) + ":" + str(command[3])
                    timestamp = int(time.time() * 1000)
                    hbase_timestamp = (2**64 - timestamp) ^ (2**63)
                    df.at[rowIndex, 'timestamp'] = hbase_timestamp
                    df.to_csv(fileName, index=False)
                    endTime = time.time() #End Timer
                    elapsedTime = endTime - startTime #Get Run time
                    print(f"1 row(s) in  {elapsedTime:.5f} seconds")

        elif os.path.exists(disabledFileName): #Disbaled file exists, throw error
            print("ERROR: Table ", command[0], " is disabled")
            print("Cannot describe a disabled table")
        elif not os.path.exists(fileName) and not os.path.exists(disabledFileName): #If both enabled and Disabled files dont exist throw error
            print("ERROR: Table ", command[0], " not found")


def InsertMany(command):
    for i in command:
        warnings.filterwarnings("ignore", message="The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.")
        startTime = time.time() #Start timer
        fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
        disabledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled

        if os.path.exists(fileName): #If file exists get data
            df = pd.read_csv(fileName)
            #Split the column family and column into another array
            commandColumn = command[2].split(":")
            #If Inputted RowId doesnt exist then create new Row
            if command[1] not in df["RowId"].values: 
                newRow = {'RowId': command[1]}
                if commandColumn[0] in df.columns:
                    newRow[commandColumn[0]] = str(commandColumn[1]) + ":" + str(command[3])
                    timestamp = int(time.time() * 1000)
                    hbase_timestamp = (2**64 - timestamp) ^ (2**63)
                    newRow['timestamp'] = hbase_timestamp
                    df = df.append(newRow, ignore_index=True)
                    df.to_csv(fileName, index=False)
                    endTime = time.time() #End Timer
                    elapsedTime = endTime - startTime #Get Run time
                    print(f"1 row(s) in  {elapsedTime:.5f} seconds")

            #If Inputted RowId exists, then edit that Row
            else:
                # Filter the rows based on the RowId and additional criteria
                mask = (df['RowId'] == command[1]) & (df[commandColumn[0]].str.contains(commandColumn[1]))
                rows = df.loc[mask]

                if len(rows) == 0:
                    # If the correct row is not found, add a new row with the given RowId
                    newRow = {'RowId': command[1]}
                    if commandColumn[0] in df.columns:
                        newRow[commandColumn[0]] = str(commandColumn[1]) + ":" + str(command[3])
                    
                    timestamp = int(time.time() * 1000)
                    hbase_timestamp = (2**64 - timestamp) ^ (2**63)
                    newRow['timestamp'] = hbase_timestamp
                    df = df.append(newRow, ignore_index=True)
                    df.to_csv(fileName, index=False)
                    endTime = time.time() #End Timer
                    elapsedTime = endTime - startTime #Get Run time
                    print(f"1 row(s) in  {elapsedTime:.5f} seconds")

                else:
                    # Edit the first row that matches the criteria
                    rowIndex = rows.index[0]
                    df.at[rowIndex, commandColumn[0]] = str(commandColumn[1]) + ":" + str(command[3])
                    timestamp = int(time.time() * 1000)
                    hbase_timestamp = (2**64 - timestamp) ^ (2**63)
                    df.at[rowIndex, 'timestamp'] = hbase_timestamp
                    df.to_csv(fileName, index=False)
                    endTime = time.time() #End Timer
                    elapsedTime = endTime - startTime #Get Run time
                    print(f"1 row(s) in  {elapsedTime:.5f} seconds")

        elif os.path.exists(disabledFileName): #Disbaled file exists, throw error
            print("ERROR: Table ", command[0], " is disabled")
            print("Cannot describe a disabled table")
        elif not os.path.exists(fileName) and not os.path.exists(disabledFileName): #If both enabled and Disabled files dont exist throw error
            print("ERROR: Table ", command[0], " not found")


#endregion