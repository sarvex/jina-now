import os

from now.compare.compare_flows import _convert_multiple_html_to_pdf


def test_convert_multiple_html_to_pdf(resources_folder_path):
    _convert_multiple_html_to_pdf(os.path.join(resources_folder_path, 'html_files'))
    assert os.path.exists(
        os.path.join(resources_folder_path, 'html_files', 'Report.pdf')
    )
