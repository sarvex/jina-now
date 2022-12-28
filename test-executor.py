from jina import Flow

with Flow().add(
    uses='jinahub+docker://CLIPOnnxEncoder/0.8.1',
    uses_with={'name': 'ViT-B-32::openai'},
    env={'JINA_LOG_LEVEL': 'DEBUG'},
) as f:
    pass
