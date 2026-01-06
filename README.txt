hybrid-pathway-rag/
├── .env                       # OPENAI_API_KEY, NEO4J_URI
├── .gitignore
├── requirements.txt           # pathway[xpack-llm], neo4j, openai
├── data/                      # Your book/code files (watched folder)
│
├── config/
│   └── schema_prompt.txt      # "Extract nodes/edges based on these rules..."
│
└── src/
    ├── __init__.py
    ├── graph_connector.py     # Custom logic to push data to Neo4j
    └── app.py                 # The MAIN entry point (Pathway pipeline)
