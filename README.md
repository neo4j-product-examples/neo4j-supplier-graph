# Neo4j Supplier Graph

A working example for deploying a GraphRAG agent to automate Bill of Materials and Supply chain analytics.

This is done using Neo4j as the graph Database and Google Cloud Platform (GCP) infrastructure including: 
1. Gemini for the LLM
2. Google Agent Development Kit (ADK) for the agent SDK
3. Google Cloud Run for agent deployment
4. BigQuery to store the source data (to be mapped to knowledge graph in Neo4j)

This repo is broken into 3 major directories: 

1. __[/create-source-data](/create-source-data) (Can Be Skipped):__  Code used to construct source data 
2. __[/knowledge-graph-creation](/knowledge-graph-creation):__ Map BigQuery tables and GCS documents to a Neo4j Knowledge Graph
3. __[/agents](/agents):__ Get started with and deploy multi-agent GraphRAG service. See README in directory for instructions

## Prerequisites
1. You should have access to a google project account and be able to authenticate through the gcloud cli. 
2. Create a Python env for running everything and install the requirements `pip install -r requirements.txt`

