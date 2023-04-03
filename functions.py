import os
import pandas as pd
import time

current_dir = os.getcwd()
subfolder_name = 'DataBase'
DataBasePath = os.path.join(current_dir, subfolder_name)

# lenguaje de definicion de datos

def create(command): 
    startTime = time.time() #Start timer
    fileName = os.path.join(DataBasePath, command[0] + '.csv') #Get the file path and file name
    disbaledFileName = os.path.join(DataBasePath, command[0] + '_Disabled.csv') #Get the file of the table if it was disabled

    if not os.path.exists(fileName): #If file doesn't exist create
        if not os.path.exists(disbaledFileName): #If disabled file doesn't exist create
            cols = command[1:] #Get column names, everything except first value
            if len(cols) == 0: #No columns specified throw error
                cols.append("Default")

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

def describe():
    pass


# lenguaje de manipulacion de datos


def put():
    pass



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



# puntos extra


def updateMany():
    pass


def InsertMany():
    pass