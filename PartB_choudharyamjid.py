from config.Neo_connect import Neo4jConnection
import pandas as pd
import config.db_settings as db_config

# stablish connection with neo4j
conn = Neo4jConnection(uri="bolt://localhost:7687", user=db_config.USER, pwd=db_config.PASSWORD)


q1 = '''
MATCH (a:article)-[:has_cited]->(c:article)-[:is_published_in]->(p:publisher{type:'Conference Paper'}) 
with p,c,count(*) as cited order by p,cited DESC 
with p, collect([c,cited]) as cite_count 
RETURN p.title as coference_name,
cite_count[0][0].title as ppr1, cite_count[0][1] as p1cited,
        cite_count[1][0].title as ppr2, cite_count[1][1] as p2cited,
        cite_count[2][0].title as ppr3, cite_count[2][1] as p3cited
'''

result = conn.query(q1, db=db_config.DATABASE)
print(result)


q2 = '''
MATCH (ar:author)-[:is_author_of]->(a:article)-[ip:is_published_in]->(p:publisher{type:'Conference Paper'})
WITH ar,p, count(distinct(ip.year)) AS co 
WHERE co>=2 
with p,collect(DISTINCT(ar.id)) as community                            
RETURN p.id as conference_id,community
'''
result = conn.query(q2, db=db_config.DATABASE)
print(result)


q3 = '''
MATCH (a:article)-[hc:has_cited]->(ar)-[ip:is_published_in]->(p:publisher{type:'Journal'})
with p,hc.year as y1,count(a) as cite
MATCH (ar:article)-[ip:is_published_in]->(p)
where toInteger(ip.year) = toInteger(y1)-1
with p, y1, cite, count(ar) as publish1
MATCH (ar:article)-[ip:is_published_in]->(p)
where toInteger(ip.year) = toInteger(y1)-2
with p, y1, cite, publish1, count(ar) as publish2
with p, avg(cite/(publish1+publish2)) as journal_index
return p.title as journal_name,journal_index order by journal_index desc
'''
result = conn.query(q3, db=db_config.DATABASE)
print(result)


q4 = '''
MATCH (art:article)-[:has_cited]->(b:article)<-[is_author_of]-(a:author) 
WITH a as author,b.id as article, count(b.id) as no_times_cited order by no_times_cited DESC  
WITH author, collect(no_times_cited) as list 
with author, [i in range(1,size(list)) where i <= list[i-1] |i] as h_index
RETURN author.name as author_name,h_index[-1] as author_index
'''
result = conn.query(q4, db=db_config.DATABASE)
print(result)