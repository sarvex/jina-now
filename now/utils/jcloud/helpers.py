import yaml
from jina.jaml import JAML


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
