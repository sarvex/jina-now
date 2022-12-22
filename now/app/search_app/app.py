from typing import Dict, List, Tuple, Union

from docarray import DocumentArray
from jina.helper import random_port

from now.app.base.app import JinaNOWApp
from now.common.utils import _extract_tags_for_indexer, get_email, get_indexer_config
from now.constants import (
    CLIP_USES,
    EXECUTOR_PREFIX,
    EXTERNAL_CLIP_HOST,
    NOW_AUTOCOMPLETE_VERSION,
    NOW_PREPROCESSOR_VERSION,
    Apps,
    Modalities,
)
from now.demo_data import DemoDatasetNames
from now.executor.name_to_id_map import name_to_id_map
from now.finetuning.run_finetuning import finetune
from now.finetuning.settings import parse_finetune_settings
from now.now_dataclasses import UserInput


class SearchApp(JinaNOWApp):
    def __init__(self):
        super().__init__()

    @property
    def app_name(self) -> str:
        return Apps.SEARCH_APP

    @property
    def is_enabled(self) -> bool:
        return True

    @property
    def description(self) -> str:
        return 'Image-text search app'

    @property
    def input_modality(self) -> Union[Modalities, List[Modalities]]:
        return [Modalities.IMAGE, Modalities.TEXT]

    @property
    def output_modality(self) -> Union[Modalities, List[Modalities]]:
        return [Modalities.IMAGE, Modalities.TEXT]

    @property
    def required_docker_memory_in_gb(self) -> int:
        return 8

    @property
    def finetune_datasets(self) -> [Tuple]:
        return DemoDatasetNames.DEEP_FASHION, DemoDatasetNames.BIRD_SPECIES

    @property
    def samples_frames_video(self) -> int:
        """Number of frames to sample from a video"""
        return 3

    @staticmethod
    def autocomplete_stub() -> Tuple[Dict, Dict]:
        """
        Returns a dictionary of autocomplete executors to be added in the flow
        """
        exec_stub = {
            'name': 'autocomplete_executor',
            'uses': '${{ ENV.AUTOCOMPLETE_EXECUTOR_NAME }}',
            'needs': 'gateway',
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
        }
        exec_env = {
            'AUTOCOMPLETE_EXECUTOR_NAME': f'{EXECUTOR_PREFIX}{name_to_id_map.get("NOWAutoCompleteExecutor2")}'
            f'/{NOW_AUTOCOMPLETE_VERSION}',
        }
        return exec_stub, exec_env

    @staticmethod
    def preprocessor_stub() -> Tuple[Dict, Dict]:
        """
        Returns a dictionary of preprocessor executors to be added in the flow
        """
        exec_stub = {
            'name': 'preprocessor',
            'replicas': '${{ ENV.PREPROCESSOR_REPLICAS }}',
            'uses': '${{ ENV.PREPROCESSOR_NAME }}',
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
            'uses_with': {
                'app': '${{ ENV.APP }}',
            },
            'jcloud': {
                'resources': {
                    'memory': '1G',
                    'cpu': '${{ ENV.PREPROCESSOR_CPU }}',
                    'capacity': 'on-demand',
                }
            },
        }
        exec_env = {
            'PREPROCESSOR_NAME': f'{EXECUTOR_PREFIX}{name_to_id_map.get("NOWPreprocessor")}/{NOW_PREPROCESSOR_VERSION}',
            'PREPROCESSOR_REPLICAS': 1,
            'PREPROCESSOR_CPU': 1,
            'APP': Apps.SEARCH_APP,
        }
        return exec_stub, exec_env

    @staticmethod
    def clip_encoder_stub(user_input) -> Tuple[Dict, Dict]:
        is_remote = user_input.deployment_type == 'remote'
        # Define the clip encoder stub
        exec_stub = {
            'name': 'clip_encoder',
            'replicas': '${{ ENV.CLIP_ENCODER_REPLICAS }}',
            'uses': f'{EXECUTOR_PREFIX}{CLIP_USES[user_input.deployment_type][0]}',
            'host': EXTERNAL_CLIP_HOST if is_remote else '0.0.0.0',
            'port': 443 if is_remote else random_port(),
            'tls': is_remote,
            'external': is_remote,
            'uses_with': {
                'name': 'ViT-B/32',
            },
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
            'needs': 'preprocessor',
        }
        # Define the clip encoder env vars
        encoder_env = {
            'CLIP_ENCODER_REPLICAS': 1,
        }
        return exec_stub, encoder_env

    @staticmethod
    def cast_convert_stub() -> Tuple[Dict, Dict]:
        exec_stub = {
            'name': 'cast_convert',
            'uses': '${{ENV.CAST_CONVERT_NAME}}',
            'uses_with': {
                'output_size': '${{ENV.PRE_TRAINED_EMBEDDINGS_SIZE}}',
                'env': {'JINA_LOG_LEVEL': 'DEBUG'},
            },
        }
        exec_env = {
            'PRE_TRAINED_EMBEDDINGS_SIZE': 512,
            'CAST_CONVERT_NAME': f'{EXECUTOR_PREFIX}CastNMoveNowExecutor/v0.0.3',
        }
        return exec_stub, exec_env

    @staticmethod
    def linear_head_stub() -> Tuple[Dict, Dict]:
        exec_stub = {
            'name': 'linear_head',
            'uses': 'jinahub+docker://FinetunerExecutor/v0.9.2',
            'uses_with': {
                'artifact': '${{ENV.FINETUNE_ARTIFACT}}',
                'token': '${{ENV.JINA_TOKEN}}',
            },
            'uses_requests': {
                '/index': 'encode',
                '/ search': 'encode',
            },
            'output_array_type': 'numpy',
            'jcloud': {'resources': {'memory': '4G'}},
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
        }
        exec_env = {}
        return exec_stub, exec_env

    @staticmethod
    def sbert_encoder_stub() -> Tuple[Dict, Dict]:
        exec_stub = {
            'name': 'sbert_encoder',
            'replicas': '${{ ENV.SBERT_ENCODER_REPLICAS }}',
            'uses': '${{ ENV.ENCODER_NAME }}',
            'host': '${{ ENV.ENCODER_HOST }}',
            'port': '${{ ENV.ENCODER_PORT }}',
            'tls': '${{ ENV.IS_REMOTE_DEPLOYMENT }}',
            'external': '${{ ENV.IS_REMOTE_DEPLOYMENT }}',
            'uses_with': {
                'name': '${{ ENV.PRE_TRAINED_MODEL_NAME }}',
            },
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
            'needs': 'preprocessor',
        }
        exec_env = {
            'SBERT_ENCODER_REPLICAS': 1,
            'ENCODER_NAME': 'jinahub+docker://SentenceTransformerEncoder',
            'ENCODER_HOST': '',
            'ENCODER_PORT': 443,
            'IS_REMOTE_DEPLOYMENT': True,
            'PRE_TRAINED_MODEL_NAME': 'paraphrase-MiniLM-L6-v2',
        }
        return exec_stub, exec_env

    @staticmethod
    def indexer_stub(user_input) -> Tuple[Dict, Dict]:
        """
        Returns a dictionary of indexers to be added in the flow
        """
        # Get indexer configuration
        indexer_config = get_indexer_config()
        # Get the filter fields to be passed to the indexer
        tags = _extract_tags_for_indexer(user_input)
        exec_stub = {
            'name': 'indexer',
            'uses': '${{ ENV.INDEXER_NAME }}',
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
            'uses_with': {
                'dim': '${{ENV.N_DIM}}',
                'columns': '${{ENV.COLUMNS}}',
            },
            'jcloud': {
                'resources': {
                    'memory': '${{ENV.INDEXER_MEM}}',
                    'cpu': '${{ENV.INDEXER_CPU}}',
                    'capacity': 'on-demand',
                }
            },
        }
        exec_env = {
            **indexer_config['indexer_resources'],
            'INDEXER_NAME': f'{EXECUTOR_PREFIX}{indexer_config["indexer_uses"]}',
            'N_DIM': CLIP_USES[user_input.deployment_type][2],
            'COLUMNS': tags,
        }
        return exec_stub, exec_env

    def finetune_setup(
        self,
        dataset: DocumentArray,
        user_input: UserInput,
        finetune_settings,
        env_dict,
        **kwargs,
    ) -> Tuple[Dict, bool]:
        kubectl_path = kwargs.get('kubectl_path', 'kubectl')
        is_finetuned = False
        if finetune_settings.perform_finetuning:
            try:
                artifact_id, token = finetune(
                    finetune_settings=finetune_settings,
                    app_instance=self,
                    dataset=dataset,
                    user_input=user_input,
                    env_dict=env_dict,
                    kubectl_path=kubectl_path,
                )

                finetune_settings.finetuned_model_artifact = artifact_id
                finetune_settings.token = token

                env_dict[
                    'FINETUNE_ARTIFACT'
                ] = finetune_settings.finetuned_model_artifact
                env_dict['JINA_TOKEN'] = finetune_settings.token
                is_finetuned = True
            except Exception:  # noqa
                print(
                    'Finetuning is currently offline. The program execution still continues without'
                    ' finetuning. Please report the following exception to us:'
                )
                import traceback

                traceback.print_exc()
                finetune_settings.perform_finetuning = False

        return env_dict, is_finetuned

    def get_executor_stubs(
        self, dataset, user_input, flow_yaml_content, **kwargs
    ) -> Tuple[Dict, Dict]:
        """
        Returns a dictionary of executors to be added in the flow along with their env vars and its values
        :param dataset: DocumentArray of the dataset
        :param user_input: user input
        :param flow_yaml_content: initial flow yaml content
        :param kwargs: additional arguments
        :return: executors stubs and env vars
        """
        if not flow_yaml_content['executors']:
            flow_yaml_content['executors'] = []
        encoders_list = []
        init_execs_list = []
        exec_env_dict = {}
        ft_encoders_list = []

        # 1. append autocomplete executor to the flow if output modality is text
        if Modalities.TEXT in self.input_modality:
            if not any(
                exec_dict['name'] == 'autocomplete_executor'
                for exec_dict in flow_yaml_content['executors']
            ):
                autocomplete, exec_env = self.autocomplete_stub()
                init_execs_list.append(autocomplete['name'])
                flow_yaml_content['executors'].append(autocomplete)
                exec_env_dict.update(exec_env)

        # 2. append preprocessors to the flow
        preprocessor, exec_env = self.preprocessor_stub()
        preprocessor['needs'] = init_execs_list[-1] if init_execs_list else 'gateway'
        init_execs_list.append(preprocessor['name'])
        flow_yaml_content['executors'].append(preprocessor)
        exec_env_dict.update(exec_env)

        # TODO: add support for finetuning of all models
        # 3a. append sbert encoder to the flow if output modalities are texts
        if Modalities.TEXT in user_input.search_mods.values():
            sbert_encoder, exec_env = self.sbert_encoder_stub()
            encoders_list.append(sbert_encoder['name'])
            sbert_encoder['needs'] = init_execs_list[-1]
            flow_yaml_content['executors'].append(sbert_encoder)
            exec_env_dict.update(exec_env)

        # 3b. append clip encoder to the flow if output modalities are images
        if (
            Modalities.IMAGE in user_input.search_mods.values()
            or Modalities.VIDEO in user_input.search_mods.values()
        ):
            # First add the clip encoder and then check if finetuning is required
            clip_encoder, exec_env = self.clip_encoder_stub(user_input)
            encoders_list.append(clip_encoder['name'])
            clip_encoder['needs'] = init_execs_list[-1]
            flow_yaml_content['executors'].append(clip_encoder)
            exec_env_dict.update(exec_env)
            exec_env_dict.update(
                {
                    'PRETRAINED_MODEL_NAME_OR_PATH': CLIP_USES[
                        user_input.deployment_type
                    ][1],
                    'PRE_TRAINED_EMBEDDINGS_SIZE': CLIP_USES[
                        user_input.deployment_type
                    ][2],
                }
            )
            # Parse finetuning settings. Should be refactored when finetuning is added
            finetune_settings = parse_finetune_settings(
                pre_trained_embedding_size=exec_env_dict['PRE_TRAINED_EMBEDDINGS_SIZE'],
                user_input=user_input,
                dataset=dataset,
                finetune_datasets=self.finetune_datasets,
                model_name='mlp',
                add_embeddings=True,
                loss='TripletMarginLoss',
            )
            # Perform the finetune setup
            exec_env_dict, is_finetuned = self.finetune_setup(
                dataset=dataset,
                user_input=user_input,
                finetune_settings=finetune_settings,
                env_dict=exec_env_dict,
                **kwargs,
            )

            if is_finetuned:
                # Cast Convert
                cast_convert, exec_env = self.cast_convert_stub()
                cast_convert['needs'] = encoders_list[-1]
                encoders_list[-1] = cast_convert['name']
                flow_yaml_content['executors'].append(cast_convert)
                exec_env_dict.update(exec_env)
                # Linear Head
                linear_head, exec_env = self.linear_head_stub()
                linear_head['needs'] = encoders_list[-1]
                encoders_list[-1] = linear_head['name']
                flow_yaml_content['executors'].append(linear_head)
                exec_env_dict.update(exec_env)

        # 4. append indexer to the flow
        if not any(
            exec_dict['name'] == 'indexer'
            for exec_dict in flow_yaml_content['executors']
        ):
            indexer_stub, exec_env = self.indexer_stub(user_input)
            # skip connection to indexer + from all encoders
            indexer_stub['needs'] = encoders_list
            flow_yaml_content['executors'].append(indexer_stub)
            exec_env_dict.update(exec_env)

        # append api_keys to all executors except the remote executors
        for executor in flow_yaml_content['executors']:
            if not (
                executor.get('external', False)
                and user_input.deployment_type == 'remote'
            ):
                if not executor.get('uses_with', None):
                    executor['uses_with'] = {}
                executor['uses_with']['api_keys'] = '${{ ENV.API_KEY }}'
                executor['uses_with']['user_emails'] = '${{ ENV.USER_EMAILS }}'
                executor['uses_with']['admin_emails'] = '${{ ENV.ADMIN_EMAILS }}'

        # Override env vars here if needed.
        is_jina_email = get_email().split('@')[-1] == 'jina.ai'
        if len(dataset) > 200_000 and is_jina_email:
            exec_env_dict.update(
                {
                    'PREPROCESSOR_REPLICAS': '20',
                }
            )

        return flow_yaml_content, exec_env_dict

    @property
    def max_request_size(self) -> int:
        """Max number of documents in one request"""
        return 10
