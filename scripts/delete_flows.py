import asyncio

from jcloud.flow import CloudFlow
from tests.integration.conftest import get_branch_name_for_flows


def create_eventloop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return asyncio.get_event_loop()


def delete_ci_flows():
    """
    Removes all flows that were created during a CI run and were not deleted
    for some reason (e.g. because the workflow was canceled).

    Since flow names include the branch name, we just iterate over all flows
    and look for those who have the current branch name inside their name, and delete only those.
    """
    print('Deleting the flows that were created during this CI run')
    branch_name = get_branch_name_for_flows()
    if branch_name != 'local-setup':
        loop = create_eventloop()
        jflows = loop.run_until_complete(CloudFlow().list_all())['flows']
        for flow in jflows:
            if flow['status']['phase'] != 'Deleted' and branch_name in flow['id']:
                CloudFlow(flow_id=flow['id']).__exit__()
                print(f"flow {flow['id']} is successfully terminated")
        print('Flows are cleaned up')
    else:
        print('This function is supposed to be called only by a CI job')


if __name__ == "__main__":
    delete_ci_flows()
