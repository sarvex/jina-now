from now.utils import get_flow_id


def test_flow_id():
    assert get_flow_id('grpcs://nowapi-92625e8747.wolf.jina.ai') == 'nowapi-92625e8747'
    assert (
        get_flow_id('grpcs://test-nowapi-92625e8747.wolf.jina.ai')
        == 'test-nowapi-92625e8747'
    )
    assert (
        get_flow_id('grpcs://something-test-nowapi-92625e8747.wolf.jina.ai')
        == 'something-test-nowapi-92625e8747'
    )
    assert (
        get_flow_id('grpcs://somethi.ng-test-nowapi-92625e8747.wolf.jina.ai')
        == 'somethi.ng-test-nowapi-92625e8747'
    )
