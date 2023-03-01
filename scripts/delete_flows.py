from jcloud.flow import CloudFlow

from now.deployment.deployment import get_or_create_eventloop, terminate_wolf
from tests.integration.conftest import get_branch_name_for_flows


def delete_ci_flows():
    """
    Removes all flows that were created during a CI run and were not deleted
    for some reason (e.g. because the workflow was canceled).

    Since flow names include the branch name, we just iterate over all flows
    and look for those who have the current branch name inside their name, and delete only those.
    """
    print('Deleting the flows that were created during this CI run')
    branch_name = get_branch_name_for_flows()
    loop = get_or_create_eventloop()
    jflows = loop.run_until_complete(CloudFlow().list_all())['flows']
    for flow in jflows:
        if flow['status']['phase'] != 'Deleted' and branch_name in flow['id']:
            terminate_wolf(flow['id'])
            print(f"flow {flow['id']} is successfully terminated")
    print('Idle flows are cleaned up')


if __name__ == "__main__":
    delete_ci_flows()
