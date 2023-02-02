from docarray import Document


def test_elasticsearch_data_loading(
    elastic_data, setup_online_shop_db, es_connection_params
):
    docs, _ = elastic_data
    assert len(docs) == 50
    assert isinstance(docs[0], Document)
    assert len(docs[0].chunks) == 1
