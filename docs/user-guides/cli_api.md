# How to use Jina NOW's CLI and API

## Use CLI Parameters

Instead of answering the questions manually, you can also provide command-line arguments when starting Jina NOW like shown here.

```bash
jina now start --quality medium --data /local/img/folder
```
  
## Use API

You can now send requests to the API using the jina client. This case shows a local deployment.

```bash
from jina import Client    
client = Client(
        host='localhost',
        port=31080,
) 
response = client.search(
        Document(text=search_text), # or in case you send an image: Document(url=image_path),
        parameters={"limit": 9, "filter": {}},
)
```
  
## Cleanup

```bash
jina now stop
```

## Requirements

- `Linux` or `Mac`
- `Python 3.7`, `3.8`, `3.9` or `3.10`

### Local execution

- `Docker` installation
- 10 GB assigned to docker
- User must be permitted to run docker containers
