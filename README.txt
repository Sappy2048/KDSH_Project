# KDSH_Project
Here's the directory guide:
hybrid-rag-core/
├── .env                       # API Keys (OpenAI, Neo4j, VectorDB)
├── .gitignore
├── docker-compose.yml         # Spins up Neo4j (Graph) and Chroma/Qdrant (Vector)
├── requirements.txt
├── README.md
│
├── config/                    # The "Control Center"
│   ├── settings.py            # Global constants (Chunk sizes, model names)
│   └── graph_schema.json      # The JSON file where your "Auto-Generated" node types live
│
├── data/
│   ├── raw/                   # Your book chapters, code files, PDFs
│   └── processed/             # (Optional) Cleaned text ready for ingestion
│
├── prompts/                   # LLM Instructions (Version Controlled)
│   ├── schema_gen.txt         # "Read this sample and invent a schema..."
│   ├── entity_extract.txt     # "Extract nodes based on graph_schema.json..."
│   └── answer_synthesis.txt   # "Combine these graph facts + vector chunks..."
│
├── scripts/                   # Standalone Utility Scripts
│   └── generate_schema.py     # Run this ONCE. It reads a sample doc -> writes graph_schema.json
│
├── src/
│   ├── __init__.py
│   ├── database.py            # Connection logic for both Neo4j and VectorDB
│   │
│   ├── ingestion/             # The "Builder" Pipeline
│   │   ├── __init__.py
│   │   ├── processor.py       # Handles text chunking
│   │   ├── vector_builder.py  # Creates embeddings -> pushes to VectorDB
│   │   └── graph_builder.py   # Extracts triplets -> pushes to Neo4j
│   │
│   └── retrieval/             # The "Searcher" Pipeline
│       ├── __init__.py
│       ├── vector_search.py   # Finds "similar" text
│       ├── graph_search.py    # Finds "connected" facts
│       └── hybrid_engine.py   # The Master Logic: Calls both + reranks results
│
└── app/                       # The Interface
    └── api.py                 # FastAPI/Streamlit entry point
