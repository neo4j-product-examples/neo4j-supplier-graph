import re
from typing import Optional, Any, List, Dict

from google.adk.agents import Agent
from dotenv import load_dotenv
import os

from neo4j import GraphDatabase
import logging

logger = logging.getLogger('agent_neo4j_cypher')
logger.info("Initializing Database for tools")

load_dotenv('.env')

NEO4J_URI = os.getenv('NEO4J_URI')
NEO4J_USERNAME = os.getenv('NEO4J_USERNAME')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')

class neo4jDatabase:
    def __init__(self,  neo4j_uri: str, neo4j_username: str, neo4j_password: str):
        """Initialize connection to the Neo4j database"""
        logger.debug(f"Initializing database connection to {neo4j_uri}")
        d = GraphDatabase.driver(neo4j_uri, auth=(neo4j_username, neo4j_password))
        d.verify_connectivity()
        self.driver = d

    def is_write_query(self, query: str) -> bool:
      return re.search(r"\b(MERGE|CREATE|SET|DELETE|REMOVE|ADD)\b", query, re.IGNORECASE) is not None

    #TODO: Use result transformer here to r: r.data()
    def _execute_query(self, query: str, params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
        """Execute a Cypher query and return results as a list of dictionaries"""
        logger.debug(f"Executing query: {query}")
        try:
            if self.is_write_query(query):
                logger.error(f"Write query not supported {query}")
                raise "Write Queries are not supported in this agent"
                # logger.debug(f"Write query affected {counters}")
                # result = self.driver.execute_query(query, params)
                # counters = vars(result.summary.counters)
                # return [counters]
            else:
                #TODO: Add Routing here for effciency
                result = self.driver.execute_query(query, params)
                results = [dict(r) for r in result.records]
                logger.debug(f"Read query returned {len(results)} rows")
                return results
        except Exception as e:
            logger.error(f"Database error executing query: {e}\n{query}")
            raise

db = neo4jDatabase(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD)

def get_schema() -> List[Dict[str,Any]]:
  """Get the schema of the database, returns node-types(labels) with their types and attributes and relationships between node-labels
  Args: None
  Returns:
    list[dict[str,Any]]: A list of dictionaries representing the schema of the database
    For example
    ```
    [{'label': 'Person','attributes': {'summary': 'STRING','id': 'STRING unique indexed', 'name': 'STRING indexed'},
      'relationships': {'HAS_PARENT': 'Person', 'HAS_CHILD': 'Person'}}]
    ```
  """
  try:
      results = db._execute_query(
              """
call apoc.meta.data() yield label, property, type, other, unique, index, elementType
where elementType = 'node' and not label starts with '_'
with label,
collect(case when type <> 'RELATIONSHIP' then [property, type + case when unique then " unique" else "" end + case when index then " indexed" else "" end] end) as attributes,
collect(case when type = 'RELATIONSHIP' then [property, head(other)] end) as relationships
RETURN label, apoc.map.fromPairs(attributes) as attributes, apoc.map.fromPairs(relationships) as relationships
              """
          )
      return results
  except Exception as e:
      return [{"error":str(e)}]


def execute_read_query(query: str, params: Optional[Dict[str, Any]]=None) -> List[Dict[str, Any]]:
    """
    Execute a Neo4j Cypher query and return results as a list of dictionaries
    Args:
        query (str): The Cypher query to execute
        params (dict[str, Any], optional): The parameters to pass to the query or None.
    Raises:
        Exception: If there is an error executing the query
    Returns:
        list[dict[str, Any]]: A list of dictionaries representing the query results
    """
    try:
        if params is None:
            params = {}
        results = db._execute_query(query, params)
        return results
    except Exception as e:
        return [{"error":str(e)}]


def get_finished_product_families() -> List[Dict[str, Any]]:
    """
    Get the different type of Product Families - for finished products only.
    Use this when asked for different types, families, or groups of finished products.
    """
    try:
        results = db._execute_query("""
        MATCH (i:Item)
        WHERE i.is_finished_product
        RETURN i.family AS family, count(*) AS numberOfProducts
        """)
        return results
    except Exception as e:
        return [{"error":str(e)}]


def get_component_families() -> List[Dict[str, Any]]:
    """
    Get the different type of Component Families - for components only -not finished products.
    Use this when asked for different types, families or groups of components and/or parts.
    """
    try:
        results = db._execute_query("""
        MATCH (i:Item)
        WHERE NOT i.is_finished_product
        RETURN i.family AS family, count(*) AS numberOfComponents
        """)
        return results
    except Exception as e:
        return [{"error":str(e)}]

def get_item(sku_id: str) -> dict[str, Any]:
    """
    Gets Item properties
    Args:
        sku_id (str): code uniquely identifying the item which can be a component or finished product
    Returns:
        dict[str, Any]: item properties
    """
    try:
        results = db._execute_query("""
        MATCH (i:Item {sku_id: "35472245A"})
        RETURN i.sku_id AS sku_id, i.name AS name, i.family AS family, i.is_finished_product AS is_finished_product
        """, {"sku_id": sku_id})
        return results
    except Exception as e:
        return {"error": str(e)}

def get_known_item_country_dependencies(sku_id: str) -> List[Dict[str, Any]]:
    """
    Retrieve all known country dependencies for a given item, which can represent either a finished product, a component, or both. This includes data on sourcing, manufacturing, or other relationships tied to specific countries.
    Dependencies are determined recursively by examining all connected components in the item's bill of materials (BOM). For each component, this function identifies:
    1. **Known Country Dependencies:** A list of countries and their corresponding dependency descriptions (e.g., sourcing steel, manufacturing).
    2. **Supply Chain Dependencies:** A visual representation of how components with such country dependencies are linked to the provided item through potentially multiple stages of the BOM hierarchy, represented as a sequence of components connected by arrows, culminating in the provided item.

    Note: This function only retrieves available data, which may be incomplete due to limited internal intelligence.

    Args:
        sku_id (str): code uniquely identifying the component or finished product
    Returns:
        list[dict[str, Any]]: a list of components in this items bill of materials with country dependencies along with their country dependencies and how they relate to the item in the bill of materials hierarchy.
    """
    try:
        results = db._execute_query("""
        MATCH (i:Item {sku_id: $sku_id})
        MATCH path=(i)<-[:BOM*]-(comp:Item)-[r:DEPENDS_ON]->(c:Country)
        RETURN
        comp.sku_id AS sku_id,
        comp.name AS name,
        collect({country: c.name, dependency: r.description}) AS known_country_dependencies,
        ' <- ' + apoc.text.join([n IN tail(nodes(path))[..-1] | n.sku_id + ' (' + coalesce(n.name, '') + ')' ], ' <- ') AS supply_chain_dependency
        """, {"sku_id": sku_id})
        return results
    except Exception as e:
        return [{"error": str(e)}]

def get_item_bill_of_materials(sku_id: str) -> List[Dict[str, Any]]:
    """
    Retrieve the complete Bill of Materials (BOM) hierarchy for a given finished product or component identified by the SKU.
    This function provides a detailed view of all components involved in the production of the specified item.
    It identifies components recursively linked to the given item through the BOM hierarchy.

    Key Features:
    1. **Bill of Material Dependencies:** Identifies all components recursively linked to the given item through the BOM hierarchy.
    2. **Dependency Path Representation:** Produces a flattened tree structure where each entry represents a sequence of components
       connected by arrows, showing the hierarchical relationships from the base components to the provided item.


    Args:
        sku_id (str): The SKU (Stock Keeping Unit) that uniquely identifies a product, component, or part.

    Returns:
        list[dict[str, Any]]: A list of dependency paths represented as strings. Each path shows the components in the BOM hierarchy
        leading to the specified item, connected by arrows (` <- `). For example:
            " <- 35472245 (CabWiringUnit_Z5OO3)"
            " <- 35472245 (CabWiringUnit_Z5OO3) <- 35475154X (CabWiringUnit_VT75O)"
            " <- 35472245 (CabWiringUnit_Z5OO3) <- 35475154X (CabWiringUnit_VT75O) <- M3571002BX (CabWiringUnit_A9FH4)"
    """

    try:
        results = db._execute_query("""
        MATCH (i:Item {sku_id: $sku_id})
        MATCH path=(i)<-[:BOM*]-(:Item)
        RETURN ' <- ' + apoc.text.join([n IN tail(nodes(path)) | n.sku_id + ' (' + coalesce(n.name, '') + ')' ], ' <- ') AS supply_chain_dependency
        """, {"sku_id": sku_id})
        return results
    except Exception as e:
        return [{"error": str(e)}]

def get_standard_country_codes_and_names() -> List[Dict[str, Any]]:
    """
    Get the standardized country codes and names that are mentioned in the BOM and supply chain data.
    Other methods that have country names and code as input will only match data if with these standardized codes/names.
    Note that not all countries may be listed, as not all countries are mentioned in intel.
    """
    try:
        results = db._execute_query("""
        MATCH (c:Country)
        RETURN c.code as code, c.name as name
        """)
        return results
    except Exception as e:
        return [{"error":str(e)}]

def get_products_with_country_dependencies(country_code:str, dependency_search_term: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Identify finished products that are affected by dependencies on a specific material or resource from a given country.

    This function traces the supply chain of finished products to specific materials or resources sourced from a particular country. By analyzing the Bill of Materials (BOM) hierarchy,
    it identifies how dependencies on components are linked to the production of finished products.

    Key Features:
    1. **Country-Specific Dependencies:** Finds components linked to a specific country and determines how their dependencies
       impact finished products.
    2. **Dependency Search:** Allows optional filtering of dependencies by a search term (e.g., "nickel").
    3. **Supply Chain Traceability:** Provides detailed paths showing how the affected components feed into finished products.

    Args:
        country_code (str): The standardized ISO 3166 alpha-3 country code (e.g., "CAN" for Canada) to trace dependencies related to a specific country.
        dependency_search_term (Optional[str]): A search term to filter specific dependencies (e.g., "nickel").
            If omitted, all dependencies for the country are considered. keep the search terms short and simple - exact matching is used for criteria

    Returns:
        List[Dict[str, Any]]: A list where each element represents a finished product impacted by the specified dependencies.
        Each entry includes:
            - **product_sku_id (str):** The SKU of the finished product.
            - **product_name (str):** The name of the finished product.
            - **dependencies (list):** A list of dictionaries, with each dictionary representing:
                - **depends_on_item_sku_id (str):** The SKU of the component causing the dependency.
                - **depends_on_item_name (str):** The name of the component causing the dependency.
                - **country_dependency_of_item (list[dict]):** A list of country dependencies associated with the component,
                  including:
                    - **dependency (str):** A description of the dependency (e.g., "sourcing nickel").
                    - **country (str):** The country associated with the dependency (e.g., "Canada").
                - **supply_chain_dependency_for_product (str):** A trace of the supply chain relationship between the component
                  and the finished product. This trace is represented as a sequence of SKUs and component names connected by arrows (e.g.,
                  " <- D0301002 (PrecisionBolt_252A1) <- 13952372 (ChassisFrame_3AC3Z)").

    Examples:
        - **Query:** country_code = "CAN", dependency_search_term = "nickel"
        - **Output:**
            [
                {
                    "product_sku_id": "35513559",
                    "product_name": "FarmTractor_VLM4F",
                    "dependencies": [
                        {
                            "depends_on_item_sku_id": "D0301002",
                            "depends_on_item_name": "PrecisionBolt_252A1",
                            "country_dependency_of_item": [
                                {
                                    "dependency": "sourcing nickel",
                                    "country": "Canada"
                                }
                            ],
                            "supply_chain_dependency_for_product": " <- D0301002 (PrecisionBolt_252A1) <- 13978746 (CabWiringUnit_EVKCK)"
                        }
                    ]
                },
                ...
            ]
    """

    try:
        if dependency_search_term is None:
            results = db._execute_query("""
            MATCH (i:Item)-[r:DEPENDS_ON]->(c:Country {code:$countryCode})
            WITH i,r,c
            MATCH path = (i)-[:BOM*]->(p {is_finished_product:true})
            WITH p.sku_id AS product_sku_id, p.name AS product_name,  i.sku_id AS depends_on_item_sku_id, i.name AS depends_on_item_name, collect({country: c.name, dependency: r.description}) AS known_country_dependency,
                ' <- ' + apoc.text.join([n IN reverse(nodes(path)[..-1]) | n.sku_id + ' (' + coalesce(n.name, '') + ')' ], ' <- ') AS supply_chain_dependency
            RETURN  product_sku_id, product_name, collect({depends_on_item_sku_id: depends_on_item_sku_id, depends_on_item_name: depends_on_item_name, country_dependency_of_item: known_country_dependency, supply_chain_dependency_for_product: supply_chain_dependency}) AS dependencies
            """, {"countryCode": country_code, "dependencySearchTerm": dependency_search_term})
        else:
            results = db._execute_query("""
            MATCH (i:Item)-[r:DEPENDS_ON]->(c:Country {code:$countryCode})
            WHERE  lower(r.description) CONTAINS lower($dependencySearchTerm)
            WITH i,r,c
            MATCH path = (i)-[:BOM*]->(p {is_finished_product:true})
            WITH p.sku_id AS product_sku_id, p.name AS product_name,  i.sku_id AS depends_on_item_sku_id, i.name AS depends_on_item_name, collect({country: c.name, dependency: r.description}) AS known_country_dependency,
                ' <- ' + apoc.text.join([n IN reverse(nodes(path)[..-1])| n.sku_id + ' (' + coalesce(n.name, '') + ')' ], ' <- ') AS supply_chain_dependency
            RETURN  product_sku_id, product_name, collect({depends_on_item_sku_id: depends_on_item_sku_id, depends_on_item_name: depends_on_item_name, country_dependency_of_item: known_country_dependency, supply_chain_dependency_for_product: supply_chain_dependency}) AS dependencies
            """, {"countryCode": country_code, "dependencySearchTerm": dependency_search_term})
        return results
    except Exception as e:
        return [{"error": str(e)}]

def get_supplier_substitutions(supplier_code: str) -> List[Dict[str, Any]]:
    """
    Retrieves substitution recommendations for items (components and/or finished products) supplied by a specific supplier.

    This function is designed to assist in scenarios where a supplier (identified by its unique code)
    is temporarily or permanently unable to operate or is otherwise designated for substitution.
    It identifies items supplied by the specified supplier and recommends other suppliers on the same BOM tier that
    can substitute those items. Recommendations are ranked by supplier proximity based on geodistance.

    Args:
        supplier_code (str): A unique code identifying the supplier in question.

    Returns:
        list[dict[str, Any]]: A list of dictionaries, each containing details for an item supplied by the specified supplier:
            - sku_id (str): The unique identifier for the item.
            - name (str): The name or description of the item.
            - recommended_supplier (dict): The closest alternative supplier for the item, including:
                - code (str): The unique code for the supplier.
                - distance (float): The geodistance (in miles) between the original supplier's location and the alternate supplier.
              This is `null` if no alternative suppliers are available.
            - other_suppliers (list[dict]): A list of additional alternative suppliers, sorted by ascending distance, with each supplier having:
                - code (str): The unique code for the supplier.
                - distance (float): The geodistance (in miles).
              This is an empty list if no additional alternatives are available.

    Example Output:
        [
            {
                "sku_id": "28710197",
                "name": "Harness_YU5KA",
                "recommended_supplier": {
                    "distance": 99.65,
                    "code": "T4J5DE"
                },
                "other_suppliers": []
            },
            {
                "sku_id": "28018274",
                "name": "Clamp_MRVR0",
                "recommended_supplier": {
                    "distance": 99.65,
                    "code": "T4J5DE"
                },
                "other_suppliers": [
                    {
                        "distance": 1376.98,
                        "code": "RO6UY3"
                    },
                    {
                        "distance": 11170.08,
                        "code": "6TIPXK"
                    }
                ]
            }
        ]

    """
    try:
        results = db._execute_query("""
        MATCH (l)<-[:LOCATED_AT]-(s:Supplier {code: $code})<-[:AT]-(i:Item)
        OPTIONAL MATCH (i)-[:AT]->(sAlt:Supplier  WHERE sAlt.tier = s.tier AND sAlt <> s)-[:LOCATED_AT]->(lAlt)
        WHERE sAlt.tier = s.tier AND sAlt <> s
        // Calculate geodistance
        WITH i, sAlt, round(point.distance(l.geo_point, lAlt.geo_point) * 0.000621371) AS distance_in_miles
        ORDER BY i, distance_in_miles ASC
        // Collect recommended supplier and other options
        WITH i, collect({code: sAlt.code, distance: distance_in_miles}) AS all_sAlt
        RETURN i.sku_id AS sku_id, i.name as name, all_sAlt[0] AS recommended_supplier, tail(all_sAlt) AS other_suppliers
        """, {"code":supplier_code})
        return results
    except Exception as e:
        return [{"error":str(e)}]

def get_finished_products_depending_on_items(sku_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Find finished products that depend on the provided item SKUs in their supply chain.

    This function identifies finished products in a supply chain that directly or indirectly rely
    on specific items (provided by their SKUs). Using the Bill of Materials (BOM) relationships,
    it traces the supply chain dependencies starting from the supplied `sku_ids` and identifies
    finished products (`is_finished_product == true`). It also generates a detailed dependency path
    for each finished product, showing how the input SKUs are linked to the finished product.

    Args:
        sku_ids (List[str]): A list of item SKUs to find dependencies for.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries where each entry corresponds to a finished product.
            Each dictionary contains:
            - `sku_id` (str): The SKU of the finished product.
            - `depends_on_item_sku_ids` (List[str]): A list of unique item SKUs from the input that this
              finished product depends on.
            - `supply_chain_dependency` (List[str]): A list of dependency paths starting from the finished
              product and tracing backward to the input SKUs. Each path is represented as a string,
              detailing the SKUs and names of items connected in the workflow. The format of each
              path is:
              ```
              "<- FinishedProductSKU (FinishedProductName) <- IntermediateSKU (IntermediateName) <- InputSKU (InputName)"
              ```
              If there are multiple paths for the same input SKU leading to the finished product, all
              paths are included.

    Example Output:
        [
            {
                "sku_id": "13867580",
                "depends_on_item_sku_ids": ["B0301002"],
                "supply_chain_dependency": [
                    " <- 13578113 (DrivePlatform_B6A4T) <- B0301002 (Bolt_W5F3Y)"
                ]
            },
            {
                "sku_id": "35750243",
                "depends_on_item_sku_ids": ["B0301002"],
                "supply_chain_dependency": [
                    " <- 13578113 (DrivePlatform_B6A4T) <- B0301002 (Bolt_W5F3Y)",
                    " <- 13599782 (MachineRig_IBA4I) <- B0301002 (Bolt_W5F3Y)"
                ]
            }
        ]

    """
    try:
        results = db._execute_query("""
        MATCH path = (i:Item)-[:BOM*]->(p {is_finished_product: true})
        WHERE i.sku_id IN $skuIds

        RETURN p.sku_id AS sku_id,
          collect(DISTINCT i.sku_id) AS depends_on_item_sku_ids,
          collect(' <- ' + apoc.text.join([n IN reverse(nodes(path)[..-1]) | n.sku_id + ' (' + coalesce(n.name, '') + ')' ], ' <- ')) AS supply_chain_dependency
        """, {"skuIds":sku_ids})
        return results
    except Exception as e:
        return [{"error":str(e)}]


MODEL="gemini-2.5-pro-preview-03-25"

database_agent = Agent(
    model=MODEL,
    name='graph_database_agent',
    instruction="""
      You are an Neo4j graph database and Cypher query expert, that must use the database schema with a user question and repeatedly generate valid cypher statements
      to execute on the database and answer the user's questions in a friendly manner in natural language.
      If in doubt the database schema is always prioritized when it comes to nodes-types (labels) or relationship-types or property names, never take the user's input at face value.
      If the user requests also render tables, charts or other artifacts with the query results.
      Always validate the correct node-labels at the end of a relationship based on the schema.

      If a query fails or doesn't return data, use the error response 3 times to try to fix the generated query and re-run it, don't return the error to the user.
      If you cannot fix the query, explain the issue to the user and apologize.

      Fetch the graph database schema first and keep it in session memory to access later for query generation.
      Keep results of previous executions in session memory and access if needed, for instance ids or other attributes of nodes to find them again
      removing the need to ask the user. This also allows for generating shorter, more focused and less error-prone queries
      to for drill downs, sequences and loops.
      If possible resolve names to primary keys or ids and use those for looking up entities.
      The schema always indicates *outgoing* relationship-types from an entity to another entity, the graph patterns read like english language.
      `company has supplier` would be the pattern `(o:Organization)-[:HAS_SUPPLIER]->(s:Organization)`

      To get the schema of a database use the `get_schema` tool without parameters. Store the response of the schema tool in session context
      to access later for query generation.

      To answer a user question generate one or more Cypher statements based on the database schema and the parts of the user question.
      If necessary resolve categorical attributes (like names, countries, industries, publications) first by retrieving them for a set of entities to translate from the user's request.
      Use the `execute_query` tool repeatedly with the Cypher statements, you MUST generate statements that use named query parameters with `$parameter` style names
      and MUST pass them as a second dictionary parameter to the tool, even if empty.
      Parameter data can come from the users requests, prior query results or additional lookup queries.
      After the data for the question has been sufficiently retrieved, pass the data and control back to the parent agent.
    """,
    tools=[
        get_schema, execute_read_query
    ]
)

bom_supplier_research_agent = Agent(
    model=MODEL,
    name='bom_supplier_research_agent',
    instruction="""
    You are an agent that has access to a database of bill of materials (BOM), supplier, product/component, and customer relationships.
    Use the provided tools to answer questions. 
    when returning information, try to always return not just the factual attribute data but also
    codes, skus, and ids to allow the other agents to investigate them more.
    """,
    tools=[
        get_schema,
        get_item,
        get_known_item_country_dependencies,
        get_item_bill_of_materials,
        get_standard_country_codes_and_names,
        get_finished_product_families,
        get_products_with_country_dependencies,
        get_component_families,
        get_supplier_substitutions,
        get_finished_products_depending_on_items
    ]
)

root_agent = Agent(
    model=MODEL,
    name='bom_supplier_agent',
    global_instruction = "",
    instruction="""
    You are an agent that has access to a database of bill of materials (BOM), supplier, product/component, and customer relationships.
    You have a set of agents to retrieve information, you should prefer the research agents over the database agent - particularly for questions around bill of materials, component & product info, and country dependencies. Only use the database agent if you have to as a fallback.
    If the user requests it, do render tables, charts or other artifacts with the research results.
    """,

    sub_agents=[bom_supplier_research_agent, database_agent]
)