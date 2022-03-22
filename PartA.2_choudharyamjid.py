from config.Neo_connect import Neo4jConnection
import pandas as pd
import config.db_settings as db_config

# stablish connection with neo4j
conn = Neo4jConnection(uri="bolt://localhost:7687", user=db_config.USER, pwd=db_config.PASSWORD)
#result = conn.query('create database {}'.format(DATABASE))



#unique constraints
#---------------------------------
q_constraints = ['''
create constraint on ()-[r:is_published_in]-() assert exists (r.year)
''',
'''create constraint on (a:author) assert a.id is unique''',
'''create constraint on (a:article) assert a.id is unique''',
'''create constraint on (a:publisher) assert a.id is unique''',
'''create constraint on (a:Keyword) assert a.id is unique'''
]

for j,re_query in enumerate(q_constraints):
    result = conn.query(re_query, db=db_config.DATABASE)
    print("query {} executed \n {} \n".format(j+1,re_query))




#create nodes
#-----------------------------------
node_queries = [
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/authors.csv' as row FIELDTERMINATOR ',' CREATE (:author {id:row.ID, name: row.name}) return row.name;
''',
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/articles.csv' as row FIELDTERMINATOR ',' CREATE (:article {id:row.ID, title:row.title, DOI:row.DOI}) return row.title;
''',
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/conference.csv' as row FIELDTERMINATOR ',' CREATE (:conference {id:row.ID, title:row.name, type:row.Type}) return row.name;
''',
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/journal.csv' as row FIELDTERMINATOR ',' CREATE (:journal {id:row.ID, title:row.name, type:row.Type}) return row.name;
''',
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/concepts.csv' as row FIELDTERMINATOR ',' CREATE (:Keyword {id:row.ID, name: row.name}) return row.name;
''']


for i, nd_query in enumerate(node_queries):
    result = conn.query(nd_query, db=db_config.DATABASE)
    print("query {} executed \n {} \n".format(i+1,nd_query))





#make_relations
#-----------------------------------------------

relation_queries = [
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/author_written_article.csv' as row FIELDTERMINATOR ','
MATCH (auth:author {id: row.author_ID}),(art:article {id: row.article_ID})
CREATE (auth)-[r:is_author_of]->(art) return r
''',
'''LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/article_published_by.csv' as row FIELDTERMINATOR ','
MATCH (art:article {id: row.article_ID}),(publ:conference {id: row.publisher_ID})
CREATE (art)-[r:is_published_in{year:row.year}]->(publ) return r''',

'''LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/article_published_by.csv' as row FIELDTERMINATOR ','
MATCH (art:article {id: row.article_ID}),(publ:journal {id: row.publisher_ID})
CREATE (art)-[r:is_published_in{year:row.year, volume:"1"}]->(publ) return r'''
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/paper_cited_by.csv' as row FIELDTERMINATOR ','
MATCH (art:article {id: row.article_ID}),(cite:article {id: row.cite_paper_ID})
CREATE (art)-[r:has_cited{year:row.year, volume:"1"}]->(cite) return r
''',
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/article_has_keyword.csv' as row FIELDTERMINATOR ','
MATCH (art:article {id: row.article_ID}),(key:Keyword {id: row.keyword_ID})
CREATE (art)-[r:has]->(key) return r
''']


for j,re_query in enumerate(relation_queries):
    result = conn.query(re_query, db=db_config.DATABASE)
    print("query {} executed \n {} \n".format(j+1,re_query))




#=======================================================================================
# creating index on publisher node
ind_q = '''CREATE INDEX conf_journ_idx FOR (pub:publisher) ON (pub.type)'''
result = conn.query(ind_q, db=db_config.DATABASE)
print("index created on publisher node on attribute type")












# q = ''' MATCH (n:article) RETURN n LIMIT 25'''
# result = conn.query(q, db=DATABASE)
# # print(result[1].data())
# dtf_data = pd.DataFrame([_.data()['n'] for _ in result])
# print(dtf_data)
# loading data from CSV
#---------------------------------------------------------------------
