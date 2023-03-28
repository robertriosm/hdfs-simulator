from functions import *
import re

print('\nwelcome to fakedfs\n')

createRegex = r'^create\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'
op = False
ye = ''
consoleDir = ">>> "


while not op:

    ye = input(consoleDir)

    # Create 
    if re.match(createRegex, ye):
        dirname = ye.split(' ')[1:]
        for i in dirname:
            if i[-1] == ',':
                dirname = i[:-1]
        

        print(dirname)

    # List


    # Disable

    # Is_enabled

    # Alter

    # Drop

    # Drop all

    # Describe

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
