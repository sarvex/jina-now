from now.utils import get_flow_id


def test_flow_id():
    assert (
        get_flow_id('https://nowapi-92625e8747-http.wolf.jina.ai')
        == 'nowapi-92625e8747'
    )
    assert (
        get_flow_id('https://test-nowapi-92625e8747-http.wolf.jina.ai')
        == 'test-nowapi-92625e8747'
    )
    assert (
        get_flow_id('https://something-test-nowapi-92625e8747-http.wolf.jina.ai')
        == 'something-test-nowapi-92625e8747'
    )
    assert (
        get_flow_id('https://somethi.ng-test-nowapi-92625e8747-http.wolf.jina.ai')
        == 'somethi.ng-test-nowapi-92625e8747'
    )
