import pathway as pw
import  warnings
import csv
from dotenv import load_dotenv
from pathway.xpacks.llm.splitters import RecursiveSplitter
from pathway.stdlib.ml.index import KNNIndex

# Import our custom connector
from graph_connector import GraphIngestor
from embedder import get_embedding
from decomposer import get_claims
from final_reasoner import generate_verdict

# Load environment variables
load_dotenv()

# Suppress the harmless "pkg_resources" warning
warnings.filterwarnings("ignore", category=UserWarning)

splitter = RecursiveSplitter(
    chunk_size=1000,
    chunk_overlap=200,
    separators=["\n\n", "\n", ".", "!", "?", " ", ""]
)

def prepare_clean_csv(input_file, output_file):
    """
    Reads the original CSV, renames the 'id' header to 'query_number',
    and saves it to a temporary file for Pathway to read safely.
    """
    try:
        with open(input_file, 'r', encoding='utf-8-sig') as f_in: # utf-8-sig handles BOM (\ufeff)
            reader = csv.reader(f_in)
            headers = next(reader) # Read the first row (headers)
            
            # Find the problematic "id" column (handling potential whitespace/BOM)
            # We look for any header that *looks* like "id"
            id_index = -1
            for i, h in enumerate(headers):
                if h.strip().lower() == "id":
                    headers[i] = "query_number" # RENAME IT!
                    id_index = i
                    break
            
            # If we couldn't find "id", just assume the first col is the ID
            if id_index == -1:
                headers[0] = "query_number"

            # Write the clean file
            with open(output_file, 'w', newline='', encoding='utf-8') as f_out:
                writer = csv.writer(f_out)
                writer.writerow(headers) # Write clean headers
                writer.writerows(reader) # Write the rest of the data
                
        print(f"✅ Created clean shadow file: {output_file}")
        
    except Exception as e:
        print(f"⚠️ Warning: Could not process CSV: {e}")

# Run the prep immediately
prepare_clean_csv("./queries.csv", "./queries_temp.csv")
# Schema for the Knowledge Base (The files you are searching AGAINST)
# We assume these are still plain text files in the ./data folder
class DocumentSchema(pw.Schema):
    path: str
    data: str

# Schema for the Query CSV (The file provided in your screenshot)
class BackstoryQuerySchema(pw.Schema):
    query_number: str 
    book_name: str
    char: str
    caption: str
    content: str  # <--- This is what we will embed and search for
    
#splits text into chunks for embedding
@pw.udf
def split_text(text: str, chunk_size: int) -> list[str]:
    if not text:
        return []
    # Simple character-based splitting
    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

#responsible for breaking the text chunk into separate claims
@pw.udf     
def decompose_udf(text: str) -> list[str]:
    return get_claims(text)

@pw.udf
def reasoner_udf(claim: str, text: str, graph: str) -> tuple[str, str]:
    return generate_verdict(claim, text, graph)

def run_pipeline():
    # 1. Define Input Source (Watched Folder)
    # mode="streaming" ensures it reacts to new files instantly
    documents=pw.io.fs.read(
        "./data",
        format="plaintext",  # <--- CHANGED FROM 'binary'
        mode="streaming",
        with_metadata=True
    )

    # 3. Initialize the Graph Ingestor
    # We initialize it once (logic inside handles connection pooling)
    ingestor = GraphIngestor()

    # 4. Apply the UDF (User Defined Function)
    # We wrap the ingestor.process method as a Pathway UDF
    @pw.udf
    def extract_and_load(text: str) -> str:
        return ingestor.process(text)
    
    # UDF for Retrieval (Searching the Graph) -- NEW
    @pw.udf
    def search_graph_udf(query: str) -> str:
        return ingestor.search(query)
    
    # --- TRACK A: VECTOR INDEX (New) ---
    
    # A.1 Chunking
    # We split the long novel into smaller chunks for vector search.
    # We include 'path' so we know which book the chunk came from.
    chunks = documents.select(
        path=pw.this._metadata["path"],
        chunk=splitter(pw.this.data) # Simple 500-char splitter
    ).flatten(pw.this.chunk) # Flattens the list of chunks into individual rows

    graph_results = documents.select(
        path=pw.this._metadata["path"],
        ingestion_status=extract_and_load(pw.this.chunk)
    )
    # A.2 Embedding
    # Apply our Gemini Embedder UDF to each chunk
    knowledge_vectors = chunks.select(
        path=pw.this.path,
        chunk_text=pw.this.chunk,
        vector=get_embedding(pw.this.chunk)
    )

    # A.3 Indexing (KNN)
    # This creates a searchable index in memory. 
    # 1. Read the CSV file containing the backstories/queries
    # mode="static" reads it once. mode="streaming" watches for new lines.
    queries = pw.io.csv.read(
        "./queries_temp.csv",  # Ensure this matches your actual filename
        schema=BackstoryQuerySchema,
        mode="static",
    )
    # NEW: Decompose Backstories into Atomic Claims
    # This turns 1 row (Backstory) -> Many rows (Claims)
    claims = queries.select(
        original_query_id=pw.this.query_number,
        character=pw.this.char,
        # We keep the original text for reference
        original_backstory=pw.this.content, 
        # The UDF returns a list ['Claim 1', 'Claim 2']
        claim_list=decompose_udf(pw.this.content) 
    ).flatten(pw.this.claim_list) # Flatten creates a new row for every item in the list
    
    query_vectors = claims.select(
        query_id=pw.this.original_query_id,
        character=pw.this.character,
        query_content=pw.this.claim_list,
        vector=get_embedding(pw.this.claim_list),
        graph_evidence=search_graph_udf(pw.this.claim_list)
    )

    # 3. Perform the Search (KNN Join)
    # This finds the 3 chunks in your Knowledge Base most similar to each backstory
   # A. Create the Indexer
    # We tell it: "Here are the vectors (knowledge_vectors.vector) 
    # and here is the data associated with them (knowledge_vectors)"
    indexer = KNNIndex(
        data=knowledge_vectors,
        n_dimensions=768,
        data_embedding=knowledge_vectors.vector
    )
    
    # 2. Query the Index
    # Use 'get_nearest_items' instead of 'query' or '_query'
    matches = indexer.get_nearest_items(
        query_vectors.vector,      # The table of queries
        k=3,                # Top 3 results
    )
    
    # 3. The "Link Back" Logic
    # We create a final table by combining the Match data with the Query data
    results = matches.join(
        query_vectors,
        matches.id == query_vectors.id  # Match the Result ID to the Query ID
    ).select(
        # Columns from the Original Query (Right side of join)
        query_vectors.query_id,
        query_vectors.character,
        claim=query_vectors.query_content,
        
        analysis=reasoner_udf(
            query_vectors.query_content, 
            matches.chunk_text, 
            query_vectors.graph_evidence
        ),
        # Columns from the Search Result (Left side of join)
        found_text=matches.chunk_text,
        found_graph_facts=query_vectors.graph_evidence,
        found_path=matches.path
    ).select(
        # Now we unpack that analysis into the final columns
        pw.this.query_id,
        pw.this.character,
        pw.this.claim,
        verdict=pw.this.analysis[0],   # Index 0 is the Verdict
        rationale=pw.this.analysis[1], # Index 1 is the Rationale
        evidence_text=pw.this.found_text,
        evidence_graph=pw.this.found_graph_facts
    )
    
    # 6. Output/Debug
    # For now, we print the status to the console so you can see it working.
    # In production, you might write this log to a file or another DB.
    pw.io.csv.write(graph_results, "ingestion_logs.csv")
    
    # Log Vector Data (Track A) - Just to verify we are getting numbers!
    #pw.io.jsonlines.write(knowledge_vectors, "logs_vector_debug.jsonl")
    
    # Write the results to a new CSV file
    pw.io.csv.write(results, "search_results_output.csv")
    
    # Run the pipeline
    pw.run()

if __name__ == "__main__":
    run_pipeline()
