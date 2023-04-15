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

GetRegex = r'^get\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
ScanRegex = r'^scan\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
DeleteRegex = r'^delete\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
DeleteAllRegex = r'^deleteall\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
CountRegex = r'^count\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'

#endregion


op = False
console_input = ''
consoleDir = ">>> "



#Creates DataBase Folder if it doesn't exist
if not os.path.exists(DataBasePath):
    os.makedirs(DataBasePath)

while not op:

    console_input = input(consoleDir)

    command = console_input.split(' ')[1:] #Split input into array
    command = [x.replace("'", "").strip(",") for x in command] #Remove extra ' and ,
    
    #region lenguaje de definicion de datos
    # Create 
    if re.match(createRegex, console_input):
        create(command)
        
    # ListS
    if re.match(listRegex, console_input):
        hlist()

    # Disable
    if re.match(disableRegex, console_input):
        disable(command)

    # Is_enabled
    if re.match(Is_enabledRegex, console_input):
        isEnabled(command)

    # Alter
    if re.match(AlterRegex, console_input): # Aun no implementado !!
        alter(command)
    # Drop
    if re.match(DropRegex, console_input):
        drop(command)

    # Drop all
    if re.match(DropAllRegex, console_input):
        dropAll()

    # Describe
    if re.match(DescribeRegex, console_input):
        describe(command)

    #endregion

    #region lenguaje de manipulacion de datos
    # Put
    if re.match(PutRegex, console_input):
        put(command)

    # Get
    if re.match(GetRegex, console_input): # Obtiene una sola linea de data
        get(command) # get '<table name>', 'row id'
    
    # Scan
    if re.match(ScanRegex, console_input): # Obtiene toda una tabla en formato HTable
        scan(command) # scan '<table name>'

    # Delete
    if re.match(DeleteRegex, console_input): # Borra toda una fila (incluyendo todas las celdas de la fila )
        delete(command) # delete '<table name>', '<row>', '<column name>', '<time stamp>'

    # DeleteAll
    if re.match(DeleteAllRegex, console_input): 
        deleteAll(command) # deletaall '<table name>', '<row>'

    # Count
    if re.match(CountRegex, console_input): # Cuenta el numero de filas de la tabla
        count(command) # count '<table name>'

    # Truncate

    #endregion

    #region puntos extras
    # Update Many

    # Insert Many

    #endregion

    if console_input == 'exit':
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
