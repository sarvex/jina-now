import asyncio
import os

from jcloud.flow import CloudFlow


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
    branch_name = os.environ.get('GITHUB_HEAD_REF')
    if branch_name:
        branch_name = branch_name.lower()[:15]
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
