"""
UNIVERSIDAD DEL VALLE DE GUATEMALA
BASES DE DATOS 2

PROYECTO 2: 
HDFS SIMULATOR

INTEGRANTES:
JUN WOO LEE, 20979
ANDRES DE LA ROCA, 20979
ROBERTO RIOS, 20979

ESTE PROGRAMA ES EL CONTROLADOR, IMPLEMENTA UN MENU SIMPLE QUE SIMULA SER UNA TERMINAL DE HDFS
"""

from functions import *
import re
from strings import *
import random


print(title)

#region regex para leer el input
createRegex = r'^create\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
listRegex = r'^list'
disableRegex = r'^disable\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
Is_enabledRegex = r'^isEnabled\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
AlterRegex = r'^alter\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
DropRegex = r'^drop\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
DropAllRegex = r'^drop_all'
DescribeRegex = r'^describe\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'


PutRegex = r'^put\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
GetRegex = r'^get\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
ScanRegex = r'^scan\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
DeleteRegex = r'^delete\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
DeleteAllRegex = r'^deleteall\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
CountRegex = r'^count\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
TruncateRegex = r'^truncate\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'


updateManyRegex = r'updateMany^\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
InsertManyRegex = r'^insertMany\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
#endregion

op = False
console_input = ''
consoleDir = ">>> "



# Creates DataBase Folder if it doesn't exist
if not os.path.exists(DataBasePath):
    os.makedirs(DataBasePath)



while not op:

    # input
    console_input = input(consoleDir)
    command = console_input.split(' ')[1:] # Split input into array
    command = [x.replace("'", "").strip(",") for x in command] # Remove extra ' and ,
    
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
        print(command)
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
    if re.match(TruncateRegex, console_input): # Cuenta el numero de filas de la tabla
        truncate(command) 


    #endregion

    if console_input == 'massiveput':
        for i in range(1000):
            command = ['massive_test', f'{random.randint(0,1000)}', f'test_column:{random.randint(0,1000)}', f'{random.randint(0,1000)}']
            put(command)

    #region puntos extras

    # Update Many
    if re.match(updateManyRegex, console_input): # Cuenta el numero de filas de la tabla
        updateMany(command) # count '<table name>'

    # Insert Many
    if re.match(InsertManyRegex, console_input): # Cuenta el numero de filas de la tabla
        InsertMany(command) # count '<table name>'

    #endregion

    if console_input == 'exit':
        op = True
        print(adios)
