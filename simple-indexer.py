from jina import Flow


with Flow(
    tracing=True,
    traces_exporter_host='localhost',
    traces_exporter_port=4317,
).add(uses='jinahub+docker://SimpleIndexer') as f:
    f.block()
