from now.constants import DEFAULT_FLOW_NAME


def create_jcloud_name(flow_name: str) -> str:
    return (
        flow_name + '-' + DEFAULT_FLOW_NAME
        if flow_name and flow_name != DEFAULT_FLOW_NAME
        else DEFAULT_FLOW_NAME
    )
