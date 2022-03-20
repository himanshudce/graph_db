from config.Neo_connect import Neo4jConnection
import pandas as pd

# define username and password
USER = "himanshudce"
PASSWORD = "Qwe@1997"
DATABASE = "publicationdb"

# stablish connection with neo4j
conn = Neo4jConnection(uri="bolt://localhost:7687", user=USER, pwd=PASSWORD)
#result = conn.query('create database {}'.format(DATABASE))


# q = ''' MATCH (n:article) RETURN n LIMIT 25'''
# result = conn.query(q, db=DATABASE)
# # print(result[1].data())
# dtf_data = pd.DataFrame([_.data()['n'] for _ in result])
# print(dtf_data)



# loading data from CSV
#---------------------------------------------------------------------



#create nodes
#-----------------------------------
node_queries = [
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/authors.csv' as row FIELDTERMINATOR ',' CREATE (:author {id:row.ID, name: row.name}) return row.name;
''',

'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/articles.csv' as row FIELDTERMINATOR ',' CREATE (:article {id:row.ID, title:row.title, volume:row.volume, DOI:row.DOI}) return row.title;
''',
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/publications.csv' as row FIELDTERMINATOR ',' CREATE (:publisher {id:row.ID, title:row.name, type:row.Type}) return row.name;
''',
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/schools.csv' as row FIELDTERMINATOR ',' CREATE (:school {id:row.ID, name: row.name}) return row.name;
''',
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/concepts.csv' as row FIELDTERMINATOR ',' CREATE (:Keyword {id:row.ID, name: row.name}) return row.name;
''']


for i, nd_query in enumerate(node_queries):
    result = conn.query(nd_query, db=DATABASE)
    print("query {} executed \n {} \n".format(i+1,nd_query))







#make_relations
#-----------------------------------------------

relation_queries = [
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/author_written_article.csv' as row FIELDTERMINATOR ','
MATCH (auth:author {id: row.author_ID}),(art:article {id: row.article_ID})
CREATE (auth)-[r:is_author_of]->(art) return r
''',
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/author_belongs_school.csv' as row FIELDTERMINATOR ','
MATCH (auth:author {id: row.author_ID}),(sch:school {id: row.org_ID})
CREATE (auth)-[r:is_belongs_to]->(sch) return r
''',
'''LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/article_published_by.csv' as row FIELDTERMINATOR ','
MATCH (art:article {id: row.article_ID}),(publ:publisher {id: row.publisher_ID})
CREATE (art)-[r:is_published_in{year:row.year}]->(publ) return r''',

'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/paper_cited_by.csv' as row FIELDTERMINATOR ','
MATCH (art:article {id: row.article_ID}),(cite:article {id: row.cite_paper_ID})
CREATE (art)-[r:has_cited{year:row.year}]->(cite) return r
''',
'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/article_reviewed_by.csv' as row FIELDTERMINATOR ','
MATCH (art:article {id: row.article_ID}),(auth:author {id: row.author_ID})
CREATE (art)-[r:is_reviewed_by{comments:row.description,decision:row.decision}]->(auth) return r
''',

'''
LOAD CSV WITH HEADERS FROM 'file:/SDM_lab1/kaggle_data_processed/article_has_keyword.csv' as row FIELDTERMINATOR ','
MATCH (art:article {id: row.article_ID}),(key:Keyword {id: row.keyword_ID})
CREATE (art)-[r:has]->(key) return r
''']


for j,re_query in enumerate(relation_queries):
    result = conn.query(re_query, db=DATABASE)
    print("query {} executed \n {} \n".format(j+1,re_query))