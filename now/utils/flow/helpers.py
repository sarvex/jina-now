import cowsay
import yaml
from jina.jaml import JAML

from now.deployment.deployment import list_all_wolf, status_wolf


def get_flow_status(action, **kwargs):
    choices = []
    # Add all remote Flows that exists with the namespace `nowapi`
    alive_flows = list_all_wolf(status='Serving')
    for flow_details in alive_flows:
        choices.append(flow_details['name'])
    if len(choices) == 0:
        cowsay.cow(f'nothing to {action}')
        return
    else:
        questions = [
            {
                'type': 'list',
                'name': 'cluster',
                'message': f'Which cluster do you want to {action}?',
                'choices': choices,
            }
        ]
        cluster = maybe_prompt_user(questions, 'cluster', **kwargs)

    flow = [x for x in alive_flows if x['name'] == cluster][0]
    flow_id = flow['id']
    _result = status_wolf(flow_id)
    if _result is None:
        print(f'❎ Flow not found in JCloud. Likely, it has been deleted already')
    return _result, flow_id, cluster


def get_flow_id(host):
    return host[len('https://') : -len('-http.wolf.jina.ai')]


class Dumper(yaml.Dumper):
    def increase_indent(self, flow=False, *args, **kwargs):
        return super().increase_indent(flow=flow, indentless=False)


def write_flow_file(flow_yaml_content, new_yaml_file_path):
    with open(new_yaml_file_path, 'w') as f:
        JAML.dump(
            flow_yaml_content,
            f,
            indent=2,
            allow_unicode=True,
            Dumper=Dumper,
        )
