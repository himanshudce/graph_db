from config.Neo_connect import Neo4jConnection
import pandas as pd
import config.db_settings as db_config



# stablish connection with neo4j
conn = Neo4jConnection(uri="bolt://localhost:7687", user=db_config.USER, pwd=db_config.PASSWORD)


#---------------------------------------------------------------
# page_rank_algoritham
querypg_a = ''' 
CALL gds.graph.create(
  'pggraph',
  'article',
  'has_cited'
)'''
querypg_b = '''CALL gds.pageRank.stream('pggraph')
YIELD nodeId,score
with gds.util.asNode(nodeId) AS N, score ORDER BY score DESC
match (N)-[r:is_published_in]->(p:publisher)
with r.year as Year, p.type as publication_type, collect([N.title,score])[..1] as top_paper
return Year, publication_type, top_paper[0][0] as paper_title order by Year, publication_type
'''
resultpg_a = conn.query(querypg_a, db=db_config.DATABASE)
resultpg_b = conn.query(querypg_b, db=db_config.DATABASE)
df = pd.DataFrame(resultpg_b)
print(df)




#---------------------------------------------------------------
# shortest path algoritham
query_shortest_path = ''' 
CALL gds.graph.create(
  'Betweenness',
  'article',
  'has_cited'
)
YIELD
  graphName AS graph, nodeProjection, nodeCount AS nodes, relationshipCount AS rels
CALL gds.betweenness.stream('Betweenness')
YIELD nodeId, score
with gds.util.asNode(nodeId) AS name, score
ORDER BY score DESC
match (name)-[:has]->(k:Keyword)
return name.title, collect(k.name)
'''

result_shpath =  conn.query(query_shortest_path, db='publicationdb')
df = pd.DataFrame(result_shpath)
print(df)
