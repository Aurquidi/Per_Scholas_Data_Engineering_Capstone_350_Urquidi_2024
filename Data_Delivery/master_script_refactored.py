import re

with open('master_script.py', 'r') as file:
    script_content = file.read()

# Replace !pip install ... with comments
refactored_content = re.sub(r'!pip install .*', r'# \g', script_content)

with open('master_script_refactored.py', 'w') as file:
    file.write(refactored_content)
