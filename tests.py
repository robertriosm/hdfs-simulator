import re
from functions import truncate

TruncateRegex = r'^truncate\s+([^,\s]+)(?:\s*,\s*([^,\s]+))*$'

console_input = input('>>> ')
command = console_input.split(' ')[1:] # Split input into array
command = [x.replace("'", "").strip(",") for x in command] # Remove extra ' and ,

truncate(command)