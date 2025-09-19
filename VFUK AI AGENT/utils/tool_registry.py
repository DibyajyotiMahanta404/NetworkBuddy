from tools.node_loader import load_nodes
 
tool_configs = [
    {
        "name": "load_nodes",
        "description": "Loads the nodes.json file. Optionally filters out healthy nodes.",
        "parameters": {
            "type": "object",
            "properties": {
                "filter_unhealthy": {
                    "type": "boolean",
                    "description": "If true, return only unhealthy nodes"
                }
            }
        },
        "function": load_nodes
    }
]
 