from functions import *
import re

print('\nwelcome to fakedfs\n')

#region regex para leer el input
createRegex = r'^create\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
listRegex = r'^list'
# listRegex = r'^list\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
disableRegex = r'^disable\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
Is_enabledRegex = r'^isEnabled\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
AlterRegex = r'^alter\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
DropRegex = r'^drop\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
DropAllRegex = r'^drop_all'
# DropAllRegex = r'^drop_all\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$' #If also doing with regex, then need to change this regex to accept drop_all only aswell
DescribeRegex = r'^describe\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'

PutRegex = r'^put\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
#endregion


op = False
ye = ''
consoleDir = ">>> "



#Creates DataBase Folder if it doesn't exist
if not os.path.exists(DataBasePath):
    os.makedirs(DataBasePath)

while not op:

    ye = input(consoleDir)

    command = ye.split(' ')[1:] #Split input into array
    command = [x.replace("'", "").strip(",") for x in command] #Remove extra ' and ,
    
    #region lenguaje de definicion de datos
    # Create 
    if re.match(createRegex, ye):
        create(command)
        
    # List
    if re.match(listRegex, ye):
        hlist()

    # Disable
    if re.match(disableRegex, ye):
        disable(command)

    # Is_enabled
    if re.match(Is_enabledRegex, ye):
        isEnabled(command)

    # Alter
    if re.match(AlterRegex, ye):
        alter(command)
    # Drop
    if re.match(DropRegex, ye):
        drop(command)

    # Drop all
    if re.match(DropAllRegex, ye):
        dropAll()

    # Describe
    if re.match(DescribeRegex, ye):
        describe(command)

    #endregion

    #region lenguaje de manipulacion de datos
    # Put
    if re.match(PutRegex, ye):
        put(command)

    # Get
    
    # Scan

    # Delete

    # DeleteAll

    # Count

    # Truncate

    #endregion

    #region puntos extras
    # Update Many

    # Insert Many

    #endregion

    if ye == 'exit':
        op = True
        print("""
                                                         
                            88 88                        
                            88 ""                        
                            88                           
        ,adPPYYba,  ,adPPYb,88 88  ,adPPYba,  ,adPPYba,  
        ""     `Y8 a8"    `Y88 88 a8"     "8a I8[    ""  
        ,adPPPPP88 8b       88 88 8b       d8  `"Y8ba,   
        88,    ,88 "8a,   ,d88 88 "8a,   ,a8" aa    ]8I  
        `"8bbdP"Y8  `"8bbdP"Y8 88  `"YbbdP"'  `"YbbdP"'  
                                                                                                   
        """)
