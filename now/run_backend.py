from now.apps.base.app import JinaNOWApp
from now.constants import Modalities
from now.data_loading.data_loading import load_data
from now.deployment.flow import _ExecutorConfig, deploy_flow
from now.dialog import UserInput
from now.finetuning.run_finetuning import finetune_now
from now.finetuning.settings import FinetuneSettings, parse_finetune_settings


def finetune_and_push_if_possible(app: JinaNOWApp):
    finetune_settings = parse_finetune_settings(user_input, dataset)
    if finetune_settings.perform_finetuning:
        print(f'🔧 Perform finetuning!')
        finetune_settings.finetuned_model_name = finetune_now(
            user_input, dataset, finetune_settings, kubectl_path
        )

    finetuning = finetune_settings.perform_finetuning

    yaml_name = get_flow_yaml_name(user_input.output_modality, finetuning)
    app.flow_yaml = os.path.join(cur_dir, 'flow', yaml_name)

    env_file = get_custom_env_file(user_input, finetune_settings, tmpdir)
    return env_file  # todo  should return a dict


def run(app: JinaNOWApp, user_input: UserInput, tmpdir, kubectl_path: str):
    """
    TODO: Write docs

    :param user_input:
    :param tmpdir:
    :param kubectl_path:
    :return:
    """
    dataset = load_data(user_input)
    env_file = app.setup(dataset, user_input)
    (
        gateway_host,
        gateway_port,
        gateway_host_internal,
        gateway_port_internal,
    ) = deploy_flow(
        user_input=user_input,
        app=app,
        env_file=env_file,
        ns='nowapi',
        index=dataset,
        tmpdir=tmpdir,
        kubectl_path=kubectl_path,
    )
    return gateway_host, gateway_port, gateway_host_internal, gateway_port_internal


def get_custom_env_file(
    user_input: UserInput,
    finetune_settings: FinetuneSettings,
    tmpdir,
):
    suffix = 'docker' if user_input.deployment_type == 'remote' else 'docker'

    indexer_name = f'jinahub+{suffix}://DocarrayIndexer'
    encoder_config = get_encoder_config(user_input)
    linear_head_name = f'jinahub+{suffix}://{finetune_settings.finetuned_model_name}'

    env_file = os.path.join(tmpdir, 'dot.env')
    with open(env_file, 'w+') as fp:
        if finetune_settings.bi_modal:
            pre_trained_embedding_size = (
                finetune_settings.pre_trained_embedding_size * 2
            )
        else:
            pre_trained_embedding_size = finetune_settings.pre_trained_embedding_size
        config_string = (
            f'ENCODER_NAME={encoder_config.uses}\n'
            f'FINETUNE_LAYER_SIZE={finetune_settings.finetune_layer_size}\n'
            f'PRE_TRAINED_EMBEDDINGS_SIZE={pre_trained_embedding_size}\n'
            f'INDEXER_NAME={indexer_name}\n'
        )
        if encoder_config.uses_with.get('pretrained_model_name_or_path'):
            config_string += f'CLIP_MODEL_NAME={encoder_config.uses_with["pretrained_model_name_or_path"]}\n'
        fp.write(config_string)
        if finetune_settings.perform_finetuning:
            fp.write(f'LINEAR_HEAD_NAME={linear_head_name}\n')

    return env_file if env_file else None


def get_flow_yaml_name(output_modality: Modalities, finetuning: bool) -> str:
    options = {
        Modalities.TEXT_TO_IMAGE: {0: 'flow-clip.yml', 1: 'ft-flow-clip.yml'},
        Modalities.MUSIC_TO_MUSIC: {1: 'ft-flow-music.yml'},
        Modalities.TEXT: {0: 'flow-clip.yml'},
    }
    return options[output_modality][finetuning]


def get_encoder_config(user_input: UserInput) -> _ExecutorConfig:
    """
    Gets the correct Executor running the pre-trained model given the user configuration.
    :param user_input: Configures user input.
    :return: Small data-transfer-object with information about the executor
    """
    if (
        user_input.output_modality == Modalities.TEXT_TO_IMAGE
        or user_input.output_modality == Modalities.TEXT
    ):
        return _ExecutorConfig(
            name='clip',
            uses=f'jinahub+docker://CLIPEncoder/v0.2.1',
            uses_with={'pretrained_model_name_or_path': user_input.model_variant},
        )
    elif user_input.output_modality == Modalities.MUSIC_TO_MUSIC:
        return _ExecutorConfig(
            name='openl3clip',
            uses=f'jinahub+docker://BiModalMusicTextEncoder',
            uses_with={},
        )
