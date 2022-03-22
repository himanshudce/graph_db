from config.Neo_connect import Neo4jConnection
import pandas as pd
import config.db_settings as db_config


# stablish connection with neo4j
conn = Neo4jConnection(uri="bolt://localhost:7687", user=db_config.USER, pwd=db_config.PASSWORD)

# community_name = input("enter the name of community")
# KEYWORDS = list(input("enter the kewords for the community with keyword and").strip().split("and"))
# community_name = "engineering"
# key_list = ["Drones","Human engineering"] 


#---------------------------------------------
#step - 1
# create a research_community with community name



q1_a = '''create (rc:research_community {name:"Design"})'''
# match with keywords and merge
q1_b = '''match (k:Keyword) where k.name in ["Design","Product design"]
match (rc:research_community) where rc.name="Design"
    merge(rc) -[:has_kewords]->(k)
    return rc,k'''

result1_a = conn.query(q1_a, db=db_config.DATABASE)
result1_b = conn.query(q1_b, db=db_config.DATABASE)



#---------------------------------------------
#step - 2
# match all the papers published in the conferences and take a count of each conference node 
# now for each of thata node find papers related to the community and kewords
# divide and check 90% constraint

q2 = '''
match (a:article) -[r:is_published_in] -> (p)
with p as pub, count(*) as all_papers_cnt
match (rc:research_community)-[:has_kewords]->(k:Keyword)<-[:has]-(a:article)-[r:is_published_in] -> (p2)
where pub.id = p2.id and rc.name="Design"
with rc,pub, pub.title as title, all_papers_cnt, count(a) as total_cm_papers
with rc,pub, pub.title as title, all_papers_cnt,total_cm_papers,(1.0*total_cm_papers/all_papers_cnt)*100 as persentage_belonging
where persentage_belonging > 30
merge (rc)-[:community_publisher]->(pub)
return pub, all_papers_cnt, total_cm_papers, persentage_belonging'''

result2 = conn.query(q2, db=db_config.DATABASE)




#---------------------------------------------
#step - 3
#  Next, we want to identify the top papers of these conferences/journals. We need to
# find the papers with the highest page rank provided the number of citations from the
# papers of the same community (papers in the conferences/journals of the db_config.DATABASE
# community). As a result we would obtain (highlight), say, the top-100 papers of the
# conferences of the db_config.DATABASE community.


q3_a = '''CALL gds.graph.create.cypher(
  'recomm_graph',
  'match (rc:research_community)-[:community_publisher]->(p)<-[:is_published_in]-(a1:article)
where rc.name="Design" return distinct id(a1) as id',
'match (rc:research_community)-[:community_publisher]->(p)<-[:is_published_in]-(a1:article)
where rc.name="Design"
match (rc)-[:community_publisher]->(p)<-[:is_published_in]-(a2:article)
match (a1)-[:has_cited]->(a2)
return id(a1) as source ,id(a2) as target'
)
YIELD graphName AS graph, nodeQuery, nodeCount AS nodes, relationshipQuery, relationshipCount AS rels
'''
q3_b = '''
CALL gds.pageRank.stream('recomm_graph')
YIELD nodeId,score
with gds.util.asNode(nodeId) as art_nodes order by score desc limit 100
match (rc:research_community{name:"Design"})
merge (rc)-[:top_comm_papers]->(art_nodes)'''

result3_a = conn.query(q3_a, db=db_config.DATABASE)
result3_b = conn.query(q3_b, db=db_config.DATABASE)






#---------------------------------------------
#step - 4
# Finally, an author of any of these top-100 papers is automatically considered a potential 
# good match to review db_config.DATABASE papers. In addition, we want to identify gurus, i.e., 
# very reputated authors that would be able to review for top conferences. We identify 
# gurus as those authors that are authors of, at least, two papers among the top-100 
# identified
# top comm authors
q4_a = '''
match (rc:research_community{name:"Design"})-[:top_comm_papers]-(art:article)<-[:is_author_of]-(aut:author)
with rc, aut as reviewer, count(art) as total_pub_art
with rc, reviewer, case when total_pub_art<2 then "reviewer" else "guru" end as rtype
merge (rc)-[:top_comm_authors{type:rtype}]->(reviewer)
return rc, reviewer, rtype'''
# q4_b = '''
# match (rc:research_community{name:"Design"})-[:top_comm_papers]-(art:article)<-[:is_author_of]-(aut:author) 
# with rc, aut as gurus, count(art) as total_pub_art where count(art)>2
# merge (rc)-[:top_comm_authors{type:"gurus"}]->(gurus)
# return rc, gurus, total_pub_art'''

result4_a = conn.query(q4_a, db=db_config.DATABASE)
# result4_b = conn.query(q4_b, db=db_config.DATABASE)

print("the top community authors are ")
for i in result4_a:
    print(i.data())



# print("the top community gurus are ")
# for i in result4_b:
#     print(i.data())
