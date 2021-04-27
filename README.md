# pghelper
Extend capabilities of psycopg2 connection class with helper functions
for safe insert, bulk insert and data streaming

# Installation
- clone repo 
- pip install . 

# Tests
## requirements 
docker
## run tests
- pip3 install -r /test/requirements.txt
- update the PGHOST environment variable (host of the postgres container) in the shell script "test.sh"
based on your system
- run the script "test.sh" from its directory

# Examples
Examples on how to use the library can be found in the test files
