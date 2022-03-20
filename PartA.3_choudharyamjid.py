from config.Neo_connect import Neo4jConnection
import pandas as pd
import config.db_settings as db_config


# stablish connection with neo4j
conn = Neo4jConnection(uri="bolt://localhost:7687", user=db_config.USER, pwd=db_config.PASSWORD)
#result = conn.query('create database {}'.format(DATABASE))




#create nodes
#-----------------------------------
node_queries = [
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/schools.csv' as row FIELDTERMINATOR ',' CREATE (:school {id:row.ID, name: row.name}) return row.name;
''']

for i, nd_query in enumerate(node_queries):
    result = conn.query(nd_query, db=db_config.DATABASE)
    print("query {} executed \n {} \n".format(i+1,nd_query))






#make_relations
#-----------------------------------------------

relation_queries = [
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/author_belongs_school.csv' as row FIELDTERMINATOR ','
MATCH (auth:author {id: row.author_ID}),(sch:school {id: row.org_ID})
CREATE (auth)-[r:is_belongs_to]->(sch) return r
''',
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/article_reviewed_by.csv' as row FIELDTERMINATOR ','
MATCH (art:article {id: row.article_ID}),(auth:author {id: row.author_ID})
CREATE (art)-[r:is_reviewed_by{comments:row.description,decision:row.decision}]->(auth) return r
''']


for j,re_query in enumerate(relation_queries):
    result = conn.query(re_query, db=db_config.DATABASE)
    print("query {} executed \n {} \n".format(j+1,re_query))