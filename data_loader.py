from config.Neo_connect import Neo4jConnection
USER = "himanshudce"
PASSWORD = "Qwe@1997"


conn = Neo4jConnection(uri="bolt://localhost:7687", user=USER, pwd=PASSWORD)
# query_string = '''
# LOAD CSV FROM 'file:/output_school.csv' as row with toInteger(row[0]) as ID, row[1] as school_name CREATE (:School {id: row.ID, class: row.school_name});
# '''
# query_string = '''
# LOAD CSV FROM 'file:/output_school.csv' as row with toInteger(row[0]) as ID, row[1] as school_name CREATE (:School {id: row.ID, class: row.school_name});
# '''
query_string = '''
create database dblp;
'''


result = conn.query(query_string, db='movies')


for i in result:
    print(i)



#LOAD CSV FROM 'file:/SDM_lab1/data/output_school.csv' AS row FIELDTERMINATOR ';' with toInteger(row[0]) as ID, row[1] as school_name CREATE (:school {id: ID, art_info:school_name }) return ID;

