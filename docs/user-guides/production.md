# Production readiness

Jina NOW supports multiple cross modal applications such as:

- Text-to-text
- Text-to-image
- Image-to-text
- Image-to-image
- Text-to-video
- Music-to-music

Jina NOW apps are production ready, our goal is to provide you with a search pipeline that is easy to integrate in your production
system. We also come with other aspects such as AWS S3 Bucket data integration, security and finetuning.

Models we use:

- SBERT : `entence-transformers/msmarco-distilbert-base-v3`
- CLIP : `openai/clip-vit-base-patch32`
- Music Encoder: `BiModalMusicTextEncoderV2`

For hardware we use AWS EC2 instances, from our experiments you can expect:

- Queries per second: 13
- Latency: 300 ms

## AWS S3 bucket support

We now support S3 Bucket, after selecting custom data you can select S3 Bucket.
Then you can provide your `AWS access key ID` and `AWS secret key`, you can provide a list of emails of anyone you want to share your flow with.

<img width="506" alt="question-s3-bucket" src="https://user-images.githubusercontent.com/40893766/191000917-88a903ec-bf0d-4206-9e9b-029401c34843.png">

Your data is automatically loaded from the S3 bucket. It will be temporary stored on EC2 machines while processing the data
but it will not be stored.

## Auto-complete suggestions

For applications with Text as input modality, we provide the `autocomplete` feature. This feature learns slowly from the user's search requests
and provides text suggestions. This is how the API looks like if you would like to integrate it into your application: 

<img width="506" alt="autocomplete-api" src="https://user-images.githubusercontent.com/40893766/196951488-7bd5e7c2-1a7b-4933-8e3d-8cedb52b14b7.png">


## Security

You can choose to secure your Flow:

<img width="506" alt="question-security" src="https://user-images.githubusercontent.com/40893766/191002151-4cb2d223-a266-45ea-87cd-57c4968059de.png">

You can also choose to grant additional users access to the Flow:

<img width="506" alt="question-other-users" src="https://user-images.githubusercontent.com/40893766/191002373-b660df33-0f36-44d4-9f81-3e8ad8e026ab.png
">

You can do this by specifying their email addresses and seperate them using commas:

<img width="506" alt="question-emails" src="https://user-images.githubusercontent.com/40893766/191002764-049037fb-6eb7-44a6-8b22-c86687b95e0f.png
">

## Finetuning

We also support finetuning, to enable this when you create your `DocumentArray` you also provide labels inside
the tags more specifically the tag `finetuner_label`. This will automatically trigger the finetuning and your embeddings
will be improved thus enhancing the quality of your final NOW search application.

More information on the training data format can be found [here](https://finetuner.jina.ai/walkthrough/create-training-data/#prepare-training-data)

## Backend for Frontend

We provide users with BFF (Backend For Frontend) API that you can call for your search application, which makes integrating to your custom
frontend easy and seamless.

<img width="506" alt="question-emails" src="https://user-images.githubusercontent.com/40893766/191207810-e1a1df17-acf3-4de2-8211-9a297b52e3a8.png
">

## How to update your API keys

To update your API keys use this simple script:

```bash
python now/admin/update_api_keys.py
```
