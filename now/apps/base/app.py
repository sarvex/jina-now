import abc
import os
from typing import Any, Callable, Dict, List, Optional, Tuple

import docker
from docarray import DocumentArray
from jina import Client
from jina.serve.runtimes.gateway.http.models import JinaRequestModel, JinaResponseModel

from now.constants import AVAILABLE_DATASET, DEFAULT_EXAMPLE_HOSTED, Modalities
from now.datasource.datasource import DemoDatasource
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
        self.flow_yaml = ''

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
    @abc.abstractmethod
    def input_modality(self) -> Modalities:
        """
        Modality used for running search queries
        """
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def output_modality(self) -> Modalities:
        """
        Modality used for indexing data
        """
        raise NotImplementedError()

    def set_flow_yaml(self, **kwargs):
        """Used to configure the flow yaml in the Jina NOW app.
        The interface is as follows:
        - if kwargs['finetuning']=True, choose finetuning flow
        - if kwargs['encode']=True, choose encoding flow (to get embeddings for finetuning)
        - temporarily introduced kwargs['dataset_len'], if app optimizes different flows to it
        """
        flow_dir = os.path.abspath(os.path.join(__file__, '..'))
        self.flow_yaml = os.path.join(flow_dir, 'flow.yml')

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
    def supported_file_types(self) -> List[str]:
        """Used to filter files in local structure or an S3 bucket."""
        return ['**']

    @property
    def example_datasource(self) -> List[DemoDatasource]:
        """
        # TODO just a prototype - needs to be implemented in the future
        Get a list of example datasets for the app.

        """
        if self.output_modality in AVAILABLE_DATASET:
            return [
                DemoDatasource(
                    id_=ds[0], display_name=ds[1], modality_folder=self.output_modality
                )
                for ds in AVAILABLE_DATASET[self.output_modality]
            ]
        else:
            return []

    @property
    def required_docker_memory_in_gb(self) -> int:
        """
        Recommended memory limit for the docker client to run this app.
        """
        return 8

    @property
    def finetune_datasets(self) -> [Tuple]:
        """Defines the list of demo datasets which are fine-tunable."""
        return ()

    def _check_docker_mem_limit(self) -> bool:
        mem_total = docker.from_env().info().get('MemTotal')
        if (
            mem_total is not None
            and mem_total / 1e9 < self.required_docker_memory_in_gb
        ):
            print(
                '🚨 Your docker container memory limit is set to ~{:.2f}GB'.format(
                    mem_total / 1e9
                )
                + f' which is below the recommended limit of {self.required_docker_memory_in_gb}GB'
                f' for the {self.app_name} app'
            )
            return False
        else:
            return True

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

    def _check_requirements(self) -> bool:
        """
        Returns true if all requirements on the system are satisfied. Else False.
        """
        return True

    def run_checks(self, user_input: UserInput) -> bool:
        req_check = self._check_requirements()
        mem_check = True
        if user_input.deployment_type != 'remote':
            mem_check = self._check_docker_mem_limit()
        return req_check and mem_check

    # TODO Remove kubectl_path. At the moment, the setup function needs kubectl because of finetuning a custom
    #  dataset with local deployment. In that case, inference is done on the k8s cluster.
    def setup(
        self, dataset: DocumentArray, user_input: UserInput, kubectl_path: str
    ) -> Dict:
        """
        Runs before the flow is deployed.
        Common use cases:
            - create a database
            - finetune a model + push the artifact
            - notify other services
            - check if starting the app is currently possible
        :param dataset:
        :param user_input: user configuration based on the given options
        :return: dict used to replace variables in flow yaml and to clean up resources after the flow is terminated
        """
        return {}

    def cleanup(self, app_config: dict) -> None:
        """
        Runs after the flow is terminated.
        Cleans up the resources created during setup.
        Common examples are:
            - delete a database
            - remove artifact
            - notify other services
        :param app_config: contains all information needed to clean up the allocated resources
        """
        pass

    def preprocess(
        self,
        da: DocumentArray,
        user_input: UserInput,
        is_indexing: Optional[bool] = False,
    ) -> DocumentArray:
        """Loads and preprocesses every document such that it is ready for finetuning/indexing."""
        return da

    def is_demo_available(self, user_input) -> bool:
        hosted_ds = DEFAULT_EXAMPLE_HOSTED.get(self.app_name, {})
        if (
            hosted_ds
            and user_input.data in hosted_ds
            and user_input.deployment_type == 'remote'
            and 'NOW_EXAMPLES' not in os.environ
            and 'NOW_CI_RUN' not in os.environ
        ):
            client = Client(
                host=f'grpcs://now-example-{self.app_name}-{user_input.data}.dev.jina.ai'.replace(
                    '_', '-'
                )
            )
            try:
                client.post('/dry_run', timeout=2)
            except ConnectionError:
                return False
            return True
        return False

    @property
    def bff_mapping_fns(
        self,
    ) -> Dict[
        str,
        Tuple[
            Any,
            Any,
            Callable[[Any], JinaRequestModel],
            Callable[[JinaResponseModel], Any],
        ],
    ]:
        """
        Apps usually have a custom input format and response which does not match the JinaRequestModel or JinaResponseModel.
        To create the mapping each Jina NOW app can implement the two bff mapping functions for each path
        - one for requests and one for responses

        :return: dictionary from path to a tuple of request model, response model,
        request mapping fn and response mapping fn.
        By default, the mapping is the identity function and the request model and respond model are the default models.
        The path is a regex expression.
        """
        return {'.*': (JinaRequestModel, JinaResponseModel, lambda x: x, lambda x: x)}

    @property
    def index_query_access_paths(self) -> str:
        """Gives access paths for indexing and searching."""
        return '@r'

    @property
    def max_request_size(self) -> int:
        """Max number of documents in one request"""
        return 32
