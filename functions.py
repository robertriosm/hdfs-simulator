import os
import pandas as pd
import time

current_dir = os.getcwd()
subfolder_name = 'DataBase'
DataBasePath = os.path.join(current_dir, subfolder_name)

#region lenguaje de definicion de datos

def create(command): 
    startTime = time.time() #Start timer
    fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
    disbaledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled

    if not os.path.exists(fileName): #If file doesn't exist create
        if not os.path.exists(disbaledFileName): #If disabled file doesn't exist create
            cols = command[1:] #Get column names, everything except first value
            if len(cols) == 0: #No columns specified throw error
                cols.append("Default")

            cols.insert(0, "RowId")
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
    disbaledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled

    if not os.path.exists(disbaledFileName): #If the disabled table doesnt exist
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
    disbaledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled

    if not os.path.exists(fileName) and not os.path.exists(disbaledFileName): #If both enabled and Disabled files dont exist throw error
        print("ERROR: Table ", command[0], " not found")
    else: #If one of them exists check which one
        if os.path.exists(fileName): #Enabled table exists, so return true
            print("true")
        elif os.path.exists(disbaledFileName): #Disabled table exists, so return false
            print("false")


def alter(command):
    '''
    Implement ways to do:
    New Column Family, the 2 different ways 
    Delete Column family
    Modify Column Family properties
    Rename coolumn Family
    '''
    pass


def drop(command):
    fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
    disbaledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled

    if not os.path.exists(fileName) and not os.path.exists(disbaledFileName): #If both enabled and Disabled files dont exist throw error
        print("ERROR: Table ", command[0], " not found")
    else: #If one of them exists check which one
        if os.path.exists(fileName): #Enabled table exists, so return error that it needs to be disabled first
            print("ERROR: Table ", command[0], " should be disabled before dropping")
        elif os.path.exists(disbaledFileName): #Disabled table exists, so delete the file
            os.remove(disbaledFileName)
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
    disbaledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled

    if os.path.exists(fileName): #If file exists get data
        df = pd.read_csv(fileName)
        print("Table", command[0], "is ENABLED\n" + command[0] + "\nCOLUMN FAMILIES DESCRIPTION")
        for column in df.columns:
            values = df[column]
            newValues = {string.split('=')[0]: string.split('=')[1] for string in values}
            
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
        print(len(df.columns), f"row(s) in  {elapsedTime:.5f} seconds")

    elif os.path.exists(disbaledFileName): #Disbaled file exists, throw error
        print("ERROR: Table ", command[0], " is disabled")
        print("Cannot describe a disabled table")
    elif not os.path.exists(fileName) and not os.path.exists(disbaledFileName): #If both enabled and Disabled files dont exist throw error
        print("ERROR: Table ", command[0], " not found")
    
#endregion
    
#region lenguaje de manipulacion de datos

def put(command):
    startTime = time.time() #Start timer
    print(command)
    fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
    disbaledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled

    if os.path.exists(fileName): #If file exists get data
        df = pd.read_csv(fileName)
        column = df.columns
        commandColumn = command[2].split(":")
        RowId = df["RowId"]
        columnValues = df[commandColumn[0]]
        if command[1] in RowId: #The RowId exists in this table
            if commandColumn[1] in df[commandColumn[0]]: #If the column exists in column family, then needs to update that column
                pass
        else: #if RowId doesnt exist in column family then needs to create it
            pass
            '''
                Try adding it this way maybe
                new_row_values = ['Dave', 40, 'Male']
                new_row = dict(zip(cols, new_row_values))
                df = pd.DataFrame(columns=cols)
                df = df.append(new_row, ignore_index=True)
            '''


def get():
    pass


def scan():
    pass


def delete():
    pass



def deleteAll():
    pass


def count():
    pass



def truncate():
    pass

#endregion

#region puntos extra


def updateMany():
    pass


def InsertMany():
    pass

#endregion