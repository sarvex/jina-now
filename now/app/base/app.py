import os
from typing import Dict, List, Optional, Tuple, TypeVar

from docarray import DocumentArray
from jina import __version__ as jina_version

from now.app.base.create_jcloud_name import create_jcloud_name
from now.app.base.preprocess import preprocess_image, preprocess_text, preprocess_video
from now.constants import DEMO_NS, NOW_GATEWAY_VERSION
from now.demo_data import DemoDataset
from now.executor.name_to_id_map import name_to_id_map
from now.now_dataclasses import DialogOptions, UserInput


class JinaNOWApp:
    """
    Interface for Jina NOW applications.
    To create a new application, you need to inherit from this class and override the methods.
    Example:
        class TextToImageApp(JinaNowApp):
            pass
    """

    def __init__(self):
        self.flow_yaml = {}

    @property
    def app_name(self) -> str:
        """
        Name of the app. Should be an enum value set in now.constants.Apps
        """
        raise NotImplementedError()

    @property
    def is_enabled(self) -> bool:
        """
        Set to True if this app is enabled for the end user.
        """
        raise NotImplementedError()

    @property
    def description(self) -> str:
        """
        Short description of the app.
        """
        return 'Jina NOW app'

    @property
    def bff(self) -> Optional[str]:
        """
        TODO This function is currently not used but already introduces the concept of custom bff
        Used to configure the bff which is used to define input and output format.
        :return: the path to the file where the bff routs are configured
        """
        return None

    @property
    def playground(self) -> Optional[str]:
        """
        TODO This function is currently not used but already introduces the concept of custom playground
        Used to configure the playground(streamlit app) where the user can run example queries
        :return: the path to the streamlit file.
        """
        return None

    @property
    def options(self) -> List[DialogOptions]:
        """
        Get the options which are used to configure the app. Base class should override this function and
        return the list of their option. Check ``DialogOptions`` for the format of the options
        On CLI the user will get a prompt and at the storefront, a GUI will be generated accordingly.
        Example:
        ``return [options.your_custom_options`]``
        :return: List[DialogOptions]
        """
        return []

    @property
    def demo_datasets(self) -> Dict[TypeVar, List[DemoDataset]]:
        """Get a list of example datasets for the app."""
        raise NotImplementedError()

    @property
    def finetune_datasets(self) -> [Tuple]:
        """Defines the list of demo datasets which are fine-tunable."""
        return ()

    def set_app_parser(self, parser, formatter) -> None:
        """
        This parser reads from the `options` property and parses it
        to form the command line arguments for app
        """
        if self.is_enabled:
            parser = parser.add_parser(
                self.app_name,
                help=self.description,
                description=f'Create an {self.app_name} app.',
                formatter_class=formatter,
            )
            for option in self.options:
                if option.is_terminal_command:
                    parser.add_argument(
                        f'--{option.name}',
                        help=option.description,
                        type=str,
                    )

    def get_gateway_stub(self, user_input, testing=False) -> Dict:
        """Returns the stub for gateway in the flow."""
        gateway_stub = {
            'uses': f'jinahub+docker://{name_to_id_map.get("NOWGateway")}/{NOW_GATEWAY_VERSION}'
            if not testing
            else 'NOWGateway',
            'protocol': ['http', 'grpc'],
            'port': [8081, 8085],
            'monitoring': True,
            'cors': True,
            'uses_with': {'user_input_dict': user_input.to_safe_dict()},
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
            'jcloud': {
                'resources': {'instance': 'C5', 'capacity': 'spot'},
            },
        }
        if 'NOW_EXAMPLES' in os.environ:
            gateway_stub['jcloud'] = {
                'custom_dns': [
                    f'{DEMO_NS.format(user_input.dataset_name.split("/")[-1])}.dev.jina.ai'
                ]
            }
        return gateway_stub

    def get_executor_stubs(self, user_input, testing=False, **kwargs) -> Dict:
        """
        Returns the stubs for the executors in the flow.
        """
        raise NotImplementedError()

    def setup(self, user_input: UserInput, testing=False, **kwargs) -> Dict:
        """Runs before the flow is deployed to setup the flow in self.flow_yaml.
        Common use cases:
            - create a database
            - finetune a model + push the artifact
            - notify other services
            - check if starting the app is currently possible
        :param user_input: user configuration based on the given options
        :param testing: use local executors if True
        """
        # Creates generic configuration such as labels in the flow
        # Keep this function as simple as possible. It should only be used to add generic configuration needed
        # for all apps. App specific configuration should be added in the app specific setup function.
        flow_yaml_content = {
            'jtype': 'Flow',
            'with': {
                'name': 'nowapi',
                'env': {'JINA_LOG_LEVEL': 'DEBUG'},
            },
            'jcloud': {
                'version': jina_version,
                'labels': {'team': 'now'},
                'name': create_jcloud_name(user_input.flow_name),
            },
            'gateway': self.get_gateway_stub(user_input, testing),
            'executors': self.get_executor_stubs(user_input, testing, **kwargs),
        }
        # Call the gateway stub function to get the gateway for the flow
        # Call the executor stubs function to get the executors for the flow
        # append user_input and api_keys to all executors except the remote executors
        user_input_dict = user_input.to_safe_dict()
        admin_emails = user_input.admin_emails or [] if user_input.secured else []
        user_emails = user_input.user_emails or [] if user_input.secured else []
        api_key = (
            [user_input.api_key] if user_input.secured and user_input.api_key else []
        )
        for executor in flow_yaml_content['executors']:
            if not executor.get('external', False):
                if not executor.get('uses_with', None):
                    executor['uses_with'] = {}
                executor['uses_with']['user_input_dict'] = user_input_dict
                executor['uses_with']['api_keys'] = api_key
                executor['uses_with']['user_emails'] = user_emails
                executor['uses_with']['admin_emails'] = admin_emails
        self.flow_yaml = self.add_telemetry_env(flow_yaml_content)

    def preprocess(
        self,
        docs: DocumentArray,
    ) -> DocumentArray:
        """Loads and preprocesses every document such that it is ready for indexing."""
        for doc in docs:
            for chunk in doc.chunks:
                try:
                    if chunk.modality == 'text':
                        preprocess_text(chunk)
                    elif chunk.modality == 'image':
                        preprocess_image(chunk)
                    elif chunk.modality == 'video':
                        preprocess_video(chunk)
                    else:
                        raise ValueError(f'Unsupported modality {chunk.modality}')
                except Exception as e:
                    chunk.summary()
                    print(e)
        return docs

    def is_demo_available(self, user_input) -> bool:
        raise NotImplementedError()

    @property
    def max_request_size(self) -> int:
        """Max number of documents in one request"""
        return 32

    @staticmethod
    def add_telemetry_env(flow_yaml_content):
        if 'JINA_OPTOUT_TELEMETRY' in os.environ:
            flow_yaml_content['gateway']['env']['JINA_OPTOUT_TELEMETRY'] = os.environ[
                'JINA_OPTOUT_TELEMETRY'
            ]
            for executor in flow_yaml_content['executors']:
                executor['env']['JINA_OPTOUT_TELEMETRY'] = os.environ[
                    'JINA_OPTOUT_TELEMETRY'
                ]
        return flow_yaml_content
