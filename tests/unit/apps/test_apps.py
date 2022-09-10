from now_common.options import _construct_app

from now.constants import Apps


def test_app_attributes():
    """Test if all essential app attributes are defined"""
    for app in Apps():
        app_instance = _construct_app(app)
        if app_instance.is_enabled:
            assert app_instance.app_name
            assert app_instance.description
            assert app_instance.input_modality
            assert app_instance.output_modality
