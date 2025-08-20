import sys
from ultipa import Edge, GraphSet, Node, UltipaConfig, Connection, Schema, Property, DBType, RequestConfig, UltipaPropertyType
from ultipa.configuration import InsertRequestConfig
from itertools import permutations

def insert_states_and_moves_in_batches(driver, batch_size=10000, insertConfig=InsertRequestConfig):
    state_nodes = []    # A list of nodes to insert
    moves_edges = []    # A list of edges to insert
    state_to_id = {}    # A dictionary to map the puzzle state (as an immutable tuple) to its node ID
    tiles = [1, 2, 3, 4, 5, 6, 7, 8, 0]
    state_counter = 1
    
    # Generates state nodes
    for perm_tuple in permutations(tiles):
        pos_list = list(perm_tuple)        
        node_id = f"s{state_counter}" # Node ID with 's' prefix and incrementing counter
        state_node = Node(id=node_id, schema="state", values={"posList": pos_list}) 
        state_nodes.append(state_node)
        state_to_id[perm_tuple] = node_id
        state_counter += 1 # Counter for next state
    
    total_states = len(state_nodes)
    print("Generated", total_states, "puzzle states")

    # Inserts state nodes in batches

    inserted_node_count = 0
    print("Inserting State nodes in batches...")
        
    for i in range(0, total_states, batch_size):
        batch = state_nodes[i:i + batch_size]
        insertResults = driver.insertNodesBatchAuto(batch, insertConfig)

        for schemaName, insertResponse in insertResults.items():
            if insertResponse.errorItems:
                print("Error items of", schemaName, "nodes:", insertResponse.errorItems)
        
        inserted_node_count += len(batch)
        print(f"Node insertion progress: {inserted_node_count}/{total_states} ({inserted_node_count/total_states:.1%})")
    
    print(f"Finished! Total nodes inserted: {inserted_node_count}")

    # Generates moves edges

    # Possible moves for the blank tile (change in row, change in col)
    moves = [(-1, 0), (1, 0), (0, -1), (0, 1)]  # Up, Down, Left, Right
    
    # This mapping helps to find the index of the blank tile on the board
    index_to_pos = {
        0: (0, 0), 1: (0, 1), 2: (0, 2),
        3: (1, 0), 4: (1, 1), 5: (1, 2),
        6: (2, 0), 7: (2, 1), 8: (2, 2)
    }

    # Iterate through each state to find possible next states
    for current_state_tuple, start_node_id in state_to_id.items():

        current_state_list = list(current_state_tuple)      
        blank_index = current_state_list.index(0)
        blank_pos = index_to_pos[blank_index]

        for move in moves:
            new_row = blank_pos[0] + move[0]
            new_col = blank_pos[1] + move[1]

            # Check if the new position is within the grid boundaries
            if 0 <= new_row <= 2 and 0 <= new_col <= 2:
                swap_index = new_row * 3 + new_col # Index of the tile to swap with the blank
                
                # New state by swapping the blank with the tile at the swap_index
                next_state_list = list(current_state_list)
                next_state_list[blank_index], next_state_list[swap_index] = \
                    next_state_list[swap_index], next_state_list[blank_index]
                next_state_tuple = tuple(next_state_list)
                end_node_id = state_to_id.get(next_state_tuple)

                # Compare the node IDs and always create the edge from the smaller to the larger
                if end_node_id and start_node_id < end_node_id:
                    moves_edge = Edge(schema="moves", fromId=start_node_id, toId=end_node_id)
                    moves_edges.append(moves_edge)

    total_moves = len(moves_edges)
    print("Generated", total_moves, "legal movements")
    
    # Inserts moves edges in batches

    inserted_edge_count = 0
    
    for i in range(0, total_moves, batch_size):
        batch = moves_edges[i:i + batch_size]
        insertResults = driver.insertEdgesBatchAuto(batch, insertConfig)

        for schemaName, insertResponse in insertResults.items():
            if insertResponse.errorItems:
                print("Error items of", schemaName, "edges:", insertResponse.errorItems)
        
        inserted_edge_count += len(batch)
        print(f"Edge insertion progress: {inserted_edge_count}/{total_moves} ({inserted_edge_count/total_moves:.1%})")
    
    print(f"Finished! Total edges inserted: {inserted_edge_count}")

# Establishes a connection to Ultipa
ultipaConfig = UltipaConfig(
    hosts = ["xxxx.eu-south-1.cloud.ultipa.com:8443"],
    username = "yourUsername",
    password= "yourPassword"
)

try:
    driver = Connection.NewConnection(defaultConfig=ultipaConfig)
except Exception as e:
    print("Failed to connect to Ultipa. Please check your hosts, username, and password.")
    sys.exit(1)

# Creates the graph
graphName = "puzzle8"
response_1 = driver.createGraph(GraphSet(name=graphName))
print("Graph creation:", response_1.status.code.name)

# Creates the node schema "state"
state = Schema(name="state", dbType=DBType.DBNODE, properties=[
    Property(name="posList", type=UltipaPropertyType.LIST, subType=[UltipaPropertyType.UINT32])
])
response_2 = driver.createSchema(state, True, RequestConfig(graph=graphName))
print("Node schema creation:", response_2.status.code.name)

# Creates the edge schema "moves"
movement = Schema(name="moves", dbType=DBType.DBEDGE)
response_3 = driver.createSchema(movement, True, RequestConfig(graph=graphName))
print("Edge schema creation:", response_3.status.code.name)

# Inserts puzzle states and legal moves
insert_states_and_moves_in_batches(driver=driver, batch_size=50000, insertConfig=InsertRequestConfig(graph=graphName))
