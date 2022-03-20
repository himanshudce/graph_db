from config.Neo_connect import Neo4jConnection
import pandas as pd
import config.db_settings as db_config

# stablish connection with neo4j
conn = Neo4jConnection(uri="bolt://localhost:7687", user=db_config.USER, pwd=db_config.PASSWORD)


q1 = '''
'''

result = conn.query(q1, db=db_config.DATABASE)
print(result)

q2 = '''
'''
result = conn.query(q2, db=db_config.DATABASE)
print(result)

q3 = '''

'''
result = conn.query(q3, db=db_config.DATABASE)
print(result)


q4 = '''

'''
result = conn.query(q4, db=db_config.DATABASE)
print(result)