# Production readiness

Jina NOW apps are production ready: Our goal is to provide you with a search pipeline that is easy to integrate in your production
system. NOW also comes with other aspects, such as AWS S3 Bucket data integration, security and finetuning.

Models we use:

- SBERT : `sentence-transformers/msmarco-distilbert-base-v3`
- CLIP : `openai/clip-vit-base-patch32`
- Music Encoder: `BiModalMusicTextEncoderV2`

For hardware we use AWS EC2 instances. From our experiments you can expect:

- Queries per second: 13
- Latency: 300 ms

## AWS S3 bucket support

We now support S3 buckets: after selecting custom data you can select S3 bucket.
Then you can provide your `AWS access key ID` and `AWS secret key`, and a list of emails of anyone you want to share your Flow with.

<img width="506" alt="question-s3-bucket" src="https://user-images.githubusercontent.com/40893766/191000917-88a903ec-bf0d-4206-9e9b-029401c34843.png">

Your data is automatically loaded from the S3 bucket. It will be temporarily stored on EC2 machines while processing the data,
but it will not be stored permanently.

## Auto-complete suggestions

For applications with text as input modality, we provide the `autocomplete` feature. This feature learns slowly from the user's search requests
and provides text suggestions. This is how the API looks like if you would like to integrate it into your application: 

<img width="506" alt="autocomplete-api" src="https://user-images.githubusercontent.com/40893766/196951488-7bd5e7c2-1a7b-4933-8e3d-8cedb52b14b7.png">


## Security

You can choose to secure your Flow:

<img width="506" alt="question-security" src="https://user-images.githubusercontent.com/40893766/191002151-4cb2d223-a266-45ea-87cd-57c4968059de.png">

You can also choose to grant additional users access to the Flow:

<img width="506" alt="question-other-users" src="https://user-images.githubusercontent.com/40893766/191002373-b660df33-0f36-44d4-9f81-3e8ad8e026ab.png
">

You can do this by specifying their email addresses and separate them using commas:

<img width="506" alt="question-emails" src="https://user-images.githubusercontent.com/40893766/191002764-049037fb-6eb7-44a6-8b22-c86687b95e0f.png
">

## Backend for Frontend

We provide a BFF (Backend For Frontend) API that you can call for your search application, which makes integrating with your custom
frontend easy and seamless.

<img width="506" alt="question-emails" src="https://user-images.githubusercontent.com/40893766/191207810-e1a1df17-acf3-4de2-8211-9a297b52e3a8.png
">

## Update your API keys

To update your API keys use this simple script:

```bash
python now/admin/update_api_keys.py
```

## Elasticsearch Index Configuration

NOW uses [Elastic Cloud](https://www.elastic.co/cloud/) as storage backend. Each new app deployment will be provisioned an index, secured with an api key,
only accessible to the indexer within  the search app. You can configure the index settings such as `shards` and `replicas` by
setting these as environment variables before deploying your app, for example:

```bash
export PROVISION_SHARDS=2
export PROVISION_REPLICAS=1
```

This will set the index to have 2 shards and 1 replica. The default values are 1 shard and 0 replicas when these
environment variables are not set.
