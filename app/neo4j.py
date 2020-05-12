from py2neo import Graph, Node, Relationship
import os
from tqdm import tqdm

'''
NEO4J_USER = os.environ['NEO4J_USER']
NEO4J_PASSWORD = os.environ['NEO4J_PASS']
NEO4J_HOST = os.environ['NEO4J_HOST']
'''

NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = '123qwe'
NEO4J_HOST = 'localhost'

class NeoGraph:
    def __init__(self):
        self.g = Graph(host=NEO4J_HOST, user= NEO4J_USER, password = NEO4J_PASSWORD)
        #self.g = Graph(password = NEO4J_PASSWORD)

    def truncate(self):
        """Remove all nodes in the graph"""
        print("----- Truncating graph -----")
        tx = self.g.begin()
        result = tx.run('MATCH (n) DETACH DELETE n')
        tx.commit()
        tx = self.g.begin()
        result = tx.run('MATCH (n) DELETE n')
        tx.commit()        
        return result

    def add_companies(self, df):
        print("----- Starting Add companies process -----")
        tx = self.g.begin()
        for _, x in tqdm(df.iterrows(), total = len(df)):
            if x['symbol'] != "NA":
                n = Node("Symbol", name = x['symbol'], company = x['name'], variation_coefficient= x['var_coef'])
            tx.create(n)
        tx.commit()
        self.g.run("CREATE INDEX ON :Symbol(name)")
        print("----- Add companies process complete -----")

    def create_links(self, df):
        print("----- Starting relationship creation process -----")
        for _, x in tqdm(df.iterrows(), total=df.shape[0]):
            cypher = f"MATCH (s1:Symbol {{name:\'{x['symbol1']}\'}}),(s2:Symbol {{name:\'{x['symbol2']}\'}}) CREATE (s1)-[:CORR {{corr : {x['cor']}, id : '{x['id']}'}}]->(s2)"
            self.g.run(cypher)
        print("-----Relationship creation process complete -----")

    def add_stock(self, df):
        print("----- Starting Add companies process -----")
        tx = self.g.begin()
        for _, x in tqdm(df.iterrows(), total = len(df)):
            if x['stockCode'] != "NA":
                n = Node("Stock", stockCode = x['stockCode'], name = x['stockName'])
            tx.create(n)
        tx.commit()
        #self.g.run("CREATE INDEX ON :Symbol(stockCode)")
        self.g.run("CREATE INDEX index_stock FOR (s:Stock) ON (s.stockCode, s.name)")
        print("----- Add companies process complete -----")

    def create_links_stock(self, df):
        print("----- Starting relationship creation process -----")
        for _, x in tqdm(df.iterrows(), total=df.shape[0]):
            #cypher = f"MATCH (s1:Stock {{stockCode:\'{x['code1']}\'}}),(s2:Stock {{stockCode:\'{x['code2']}\'}}) CREATE (s1)-[:CORR {{corr : {x['corr']}}}]->(s2)"
            cypher = f"MATCH (s1:Stock {{stockCode:\'{x['code1']}\'}}),(s2:Stock {{stockCode:\'{x['code2']}\'}}) CREATE (s1)-[:CORR {{corr : {x['corr']}}}]->(s2)"
            self.g.run(cypher)
        print("-----Relationship creation process complete -----")


    def add_company(self, df):
        tx = self.g.begin()
        result = tx.run('MATCH (n:Company) DETACH DELETE n')
        tx.commit()

        print("----- Starting Add companies process -----")
        tx = self.g.begin()
        for _, x in tqdm(df.iterrows(), total = len(df)):
            if x['stock_code'] != "NA":
                n = Node("Company", stockCode = x['stock_code'], name = x['stock_name'], idnum=x['idnum'])
            tx.create(n)
        tx.commit()
        #self.g.run("CREATE INDEX ON :Symbol(stockCode)")
        #self.g.run("CREATE INDEX index_company FOR (s:Company) ON (s.stockCode, s.name)")
        print("----- Add companies process complete -----")

    def add_person(self, df):
        print("----- Starting Add Person process -----")
        tx = self.g.begin()
        result = tx.run('MATCH (n:Person) DETACH DELETE n')
        tx.commit()

        tx = self.g.begin()
        for _, x in tqdm(df.iterrows(), total = len(df)):
            if x['name'] != "NA":
                n = Node("Person", name = x['name'], idnum=x['idnum'])
            tx.create(n)
        tx.commit()
        #self.g.run("CREATE INDEX ON :Symbol(stockCode)")
        #self.g.run("CREATE INDEX index_person FOR (s:Person) ON (s.idnum, s.name)")
        print("----- Add Person process complete -----")

    def create_links_holder(self, df):
        print("----- Starting relationship creation process -----")
        for _, x in tqdm(df.iterrows(), total=df.shape[0]):
            idnum = x['idnum']
            if len(idnum) < 10:
                cypher = f"MATCH (s1:Person {{idnum:\'{x['idnum']}\'}}),(s2:Company {{stockCode:\'{x['stockCode']}\'}}) CREATE (s1)-[:HOLD {{ratio : {x['ratio']}}}]->(s2)"
            else:
                cypher = f"MATCH (s1:Company {{idnum:\'{x['idnum']}\'}}),(s2:Company {{stockCode:\'{x['stockCode']}\'}}) CREATE (s1)-[:HOLD {{ratio : {x['ratio']}}}]->(s2)"
            #print(cypher)
            self.g.run(cypher)
        print("-----Relationship creation process complete -----")

