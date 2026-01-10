#This program simply fetches the important nodal entities and relations using an llm query and pushes them into the driver instance
#for more information on neo4J visit https://neo4j.com/docs/python-manual/current/
import os
import json
from google import genai
from google.genai import types
from neo4j import GraphDatabase
from dotenv import load_dotenv


load_dotenv()

class GraphIngestor():
    def __init__(self):
        uri = os.getenv('NEO4J_URI')
        user = os.getenv('NEO4J_USER')
        password = os.getenv('NEO4J_PASSWORD')
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
    
    def search(self, query: str) -> str:
        """
        Extracts entities from a claim and finds connections in Neo4j.
        """
        if not query or not self.driver:
            return ""

        # 1. Extract Entities using Gemini
        # We ask for a simple JSON list of names [ "Tom", "Ship" ]
        extract_prompt = f"""
        Identify the main entities (People, Places, Objects) in this text.
        Text: "{query}"
        Return ONLY a JSON list of strings. Example: ["Tom Ayrton", "Britannia"]
        """
        try:
            response = self.client.models.generate_content(
                model="gemini-2.0-flash", 
                contents=extract_prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            entities = json.loads(response.text)
            # Ensure it's a list (handle single string case)
            if isinstance(entities, str): entities = [entities]
        except Exception as e:
            return f"Entity Extraction Error: {e}"

        if not entities:
            return "No entities found."

        # 2. Query Neo4j
        # We look for nodes where 'id' matches (case-insensitive)
        cypher_query = """
        MATCH (n:Entity)-[r:RELATIONSHIP]-(m:Entity)
        WHERE toLower(n.id) IN [x IN $names | toLower(x)]
        RETURN n.id AS source, r.type AS rel_type, m.id AS target
        LIMIT 10
        """
        
        try:
            with self.driver.session() as session:
                result = session.run(cypher_query, names=entities)
                # Format: "Tom --[LEADER]--> Mutineers"
                facts = [f"{rec['source']} --[{rec['rel_type']}]--> {rec['target']}" for rec in result]
                
            if not facts:
                return "No graph connections found."
            return "; ".join(facts)
            
        except Exception as e:
            return f"Graph Search Error: {e}"
        
    def close(self):
        if self.driver:
            self.driver.close()
        
