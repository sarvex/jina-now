from now.now_dataclasses import (
    Task,
    EncoderConfig,
    TrainDataGenerationConfig,
    TrainDataGeneratorConfig,
)
import json


def test_parse_task_config(get_config_path):
    config_path = get_config_path
    with open(config_path) as f:
        dct = json.load(f)
        task = Task(**dct)
    assert task.name == 'wordlift-multi-modal-search'
    assert task.extracted_dataset == 'extracted-wordlift-data'
    assert len(task.encoders) == 2
    assert type(task.encoders[0]) == EncoderConfig
    assert task.encoders[0].encoder_type == 'text-to-text'
    assert len(task.encoders[0].training_data_generation_methods) == 1
    assert (
        type(task.encoders[0].training_data_generation_methods[0])
        == TrainDataGenerationConfig
    )
    assert (
        type(task.encoders[0].training_data_generation_methods[0].query)
        == TrainDataGeneratorConfig
    )
