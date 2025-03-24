from typing import Any, Dict, List
import dataiku

from typing import List, Dict
import dataiku

def list_available_connections() -> Dict[str, List[Dict[str, str]]]:
    """Lists all available Dataiku connections and returns them in a structured format."""
    client = dataiku.api_client()
    connections = client.list_connections()
    
    connection_choices: List[Dict[str, str]] = []
    allowed_types = {"EC2", "Filesystem", "GCS", "Azure"}
    
    for conn in connections:
        connection_settings = client.get_connection(conn).get_settings()
        connection_type = connection_settings.type  # Get connection type
        allow_write = connection_settings.allow_write
        allow_managed_folders = connection_settings.allow_managed_folders
        
        if connection_type in allowed_types and allow_write and allow_managed_folders:
            connection_choices.append({
                "value": conn,
                "label": f"{conn} ({connection_type})"
            })

    return {"choices": connection_choices}


def do(payload, config, plugin_config, inputs):
    parameter_name = payload["parameterName"]

    if parameter_name == "connection":
        return list_available_connections()
    else:
        return {
            "choices": [
                {
                    "value": "wrong",
                    "label": f"Problem getting the name of the parameter.",
                }
            ]
        }