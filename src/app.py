#This program simply fetches the important nodal entities and relations using an llm query and pushes them into the driver instance
#for more information on neo4J visit https://neo4j.com/docs/python-manual/current/
import os
import json
from google import genai
from google.genai import types
from neo4j import GraphDatabase
from dotenv import load_dotenv
import networkx as nx #remove in final draft
import matplotlib.pyplot as plt #remove in final draft

load_dotenv()

class GraphIngestor():
    def __init__(self):
        uri = "neo4j://127.0.0.1:7687"
        user = "neo4j"
        password = "kdsh@123"
        try:
            self.driver = GraphDatabase.driver( #launches a driver instance, relying on local now switch to docker
                uri,
                auth=(user,password)
            )
            self.driver.verify_connectivity() #throw an error is unsuccessful
        except Exception as e:
            print(f"Neo4j connection failed: {e}")
            self.driver = None
        self.client=genai.Client(api_key=os.getenv('GEMINI_API_KEY')) #Setup the llm to generate nodes, in this case gemini
        self.system_instruction = self._load_prompt() #load the llm prompt into this var

    def _load_prompt(self): #Reads the prompt from config/schema_prompt
        try:
            prompt_path = os.path.join(os.path.dirname(__file__), "../config/schema_prompt.txt")
            with open(prompt_path, "r") as f:
                return f.read()
        except:
            print("Prompt file not found. Using default prompt") #if the prompt file is not found, use the default prompt
            return "Extract the entities and relations in this text in JSON format"
        

    def _push_to_db(self,json_data):
        """This method runs the cypher query using the json data. """
        if not  self.driver or not json_data:
            return "(no driver or empty data)"
        query = """
        UNWIND $nodes AS n
        MERGE (e:Entity {id: n.id})
        SET e.label = n.type
        
        WITH e
        UNWIND $edges AS r
        MATCH (source:Entity {id: r.source})
        MATCH (target:Entity {id: r.target})
        MERGE (source)-[rel:RELATIONSHIP {type: r.relation}]->(target)
        """
        
        try:
            with self.driver.session() as session:
                session.run(query, 
                            nodes=json_data.get('nodes', []), 
                            edges=json_data.get('edges', []))
            return "Success"
        except Exception as e:
            return f"Neo4j Error: {str(e)}"
        
    def process(self,text_chunk):
        try:
            response = self.client.models.generate_content(
                    model="gemini-2.5-flash-lite",
                    contents=text_chunk,
                    config=types.GenerateContentConfig(
                        system_instruction=self.system_instruction,
                        response_mime_type="application/json"
                    )
                )
            graph_data = json.loads(response.text)
            status = self._push_to_db(graph_data)
            return status
        except Exception as e:
            return f"Pipeline Error: {e}"
        
    def close(self):
        if self.driver:
            self.driver.close()
            
    def visualize_graph(self): #remove in final draft
        # 2. Fetch Data (Limit to 50 edges so the plot isn't a mess)
        query = """
        MATCH (s)-[r]->(t)
        RETURN s.id AS source, type(r) AS edge, t.id AS target
        LIMIT 50
        """
        
        results = []
        with self.driver.session() as session:
            results = session.run(query).data()
    
        if not results:
            print("Graph is empty! Run the ingestion first.")
            return
    
        # 3. Build NetworkX Graph
        G = nx.DiGraph() # Directed Graph
        
        for row in results:
            G.add_edge(row['source'], row['target'], label=row['edge'])
    
        # 4. Draw it
        plt.figure(figsize=(10, 8))
        pos = nx.spring_layout(G, k=0.5) # k controls the distance between nodes
        
        # Draw nodes and edges
        nx.draw(G, pos, 
                with_labels=True, 
                node_size=2000, 
                node_color="lightblue", 
                font_size=10, 
                font_weight="bold", 
                arrows=True)
        
        # Draw edge labels (relationships)
        edge_labels = nx.get_edge_attributes(G, 'label')
        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color='red')
        
        plt.title("Knowledge Graph Preview (First 50 Relations)")
        plt.show()
        
graph_ingestor=GraphIngestor()
graph_ingestor._load_prompt()
text_chunk="Before the law sits a gatekeeper. To this gatekeeper comes a man from the country who asks to gain entry into the law. But the gatekeeper says that he cannot grant him entry at the moment. The man thinks about it and then asks if he will be allowed to come in later on. “It is possible,” says the gatekeeper, “but not now.” At the moment the gate to the law stands open, as always, and the gatekeeper walks to the side, so the man bends over in order to see through the gate into the inside. When the gatekeeper notices that, he laughs and says: “If it tempts you so much, try it in spite of my prohibition. But take note: I am powerful. And I am only the most lowly gatekeeper. But from room to room stand gatekeepers, each more powerful than the other. I can’t endure even one glimpse of the third.” The man from the country has not expected such difficulties: the law should always be accessible for everyone, he thinks, but as he now looks more closely at the gatekeeper in his fur coat, at his large pointed nose and his long, thin, black Tartar’s beard, he decides that it would be better to wait until he gets permission to go inside. The gatekeeper gives him a stool and allows him to sit down at the side in front of the gate. There he sits for days and years. He makes many attempts to be let in, and he wears the gatekeeper out with his requests. The gatekeeper often interrogates him briefly, questioning him about his homeland and many other things, but they are indifferent questions, the kind great men put, and at the end he always tells him once more that he cannot let him inside yet. The man, who has equipped himself with many things for his journey, spends everything, no matter how valuable, to win over the gatekeeper. The latter takes it all but, as he does so, says, “I am taking this only so that you do not think you have failed to do anything.” During the many years the man observes the gatekeeper almost continuously. He forgets the other gatekeepers, and this one seems to him the only obstacle for entry into the law. He curses the unlucky circumstance, in the first years thoughtlessly and out loud, later, as he grows old, he still mumbles to himself. He becomes childish and, since in the long years studying the gatekeeper he has come to know the fleas in his fur collar, he even asks the fleas to help him persuade the gatekeeper. Finally his eyesight grows weak, and he does not know whether things are really darker around him or whether his eyes are merely deceiving him. But he recognizes now in the darkness an illumination which breaks inextinguishably out of the gateway to the law. Now he no longer has much time to live. Before his death he gathers in his head all his experiences of the entire time up into one question which he has not yet put to the gatekeeper. He waves to him, since he can no longer lift up his stiffening body."
print(graph_ingestor.process(text_chunk))
graph_ingestor.visualize_graph()
graph_ingestor.close()

