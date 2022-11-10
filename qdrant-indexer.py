from jina import Flow


with Flow().add(
    uses='docker://qdrantindexer15:local',
    uses_with={
        'dim': 512,
        'columns': [
            "content_type",
            "str",
            "finetuner_label",
            "str",
            "split",
            "str",
            "color",
            "str",
        ],
        'admin_emails': [],
        "user_emails": [],
        "api_keys": [],
    },
) as f:
    f.save_config('qdrantindexer-flow.yml')
    f.block()
