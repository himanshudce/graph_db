from config.Neo_connect import Neo4jConnection
import pandas as pd

# define username and password
USER = "himanshudce"
PASSWORD = "Qwe@1997"
DATABASE = "publicationdb"

# stablish connection with neo4j
conn = Neo4jConnection(uri="bolt://localhost:7687", user=USER, pwd=PASSWORD)


q1 = '''
'''

result = conn.query(q1, db=DATABASE)
print(result)

q2 = '''
'''
result = conn.query(q2, db=DATABASE)
print(result)

q3 = '''

'''
result = conn.query(q3, db=DATABASE)
print(result)


q4 = '''

'''
result = conn.query(q4, db=DATABASE)
print(result)


