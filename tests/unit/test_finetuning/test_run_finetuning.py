from now.finetuning.run_finetuning import get_model_options
from now.finetuning.settings import FinetuneSettings


def test_get_model_options():
    settings = FinetuneSettings(
        model_name='mlp',
        add_embeddings=True,
        loss='TripletMarginLoss',
        pre_trained_embedding_size=512,
        perform_finetuning=True,
        bi_modal=True,
    )
    input_size = settings.pre_trained_embedding_size * 2
    expected_options = {
        'input_size': input_size,
        'hidden_sizes': settings.hidden_sizes,
        'l2': True,
        'bias': False if settings.bi_modal else True,
    }
    options = get_model_options(settings)
    assert expected_options == options
    settings.model_name = 'not_mlp'
    expected_options = {}
    options = get_model_options(settings)
    assert expected_options == options
