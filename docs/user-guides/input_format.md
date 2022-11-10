# Input Formats

When deploying your app, you will be asked to select the input format of your data. 
This is the format of the data that you will be sending to the API. 
We currently support the following input options for custom input: DocumentArray, DocumentArrayUrl, local path, S3 bucket.
In the following, we will explain each of these options in more detail.

## DocumentArray
If you have your data as a `DocumentArray` you can use this option.
More information about how to create and push your own `DocumentArray` can be found [here](https://docarray.jina.ai/).

## DocumentArray Url
Similar to the `DocumentArray` option, but instead of pushing your `DocumentArray` to the API, you can provide a URL to your data.

## Local Folder
If you have your data stored locally, you can provide the path to the folder containing your data.
The folder should contain all files that you want to index.

Example structure for a text-to-image search app:
```
usr
├── data
│   ├── images
│   │   ├── 1.jpg
│   │   ├── 2.jpg
│   │   ├── 3.jpg
│   │   ├── 4.jpg
```
The local path you provide should be `/usr/data/images`.

## AWS S3 bucket
If you have your data stored in an AWS S3 bucket, you can provide the S3 URI, your `AWS access key ID` and `AWS secret key`.
Similar to the local folder option, the S3 bucket should contain all files that you want to index.
The only difference is that the S3 Uri should be in the following format: `s3://<bucket-name>/<path-to-data>`.
Taking the example structure from above, the S3 URI would be `s3://my-<bucket-name>/usr/data/images`.


# Support for Tags
For most professional applications, you will want to provide additional information about your data, such as price, creator, brand, etc.
This information can be provided in the `tags` of your `Documents` in case you selected the `DocumentArray` or `DocumentArrayUrl` option.
If you selected the `local folder` or `S3 bucket` option, you can provide this information in a `json` file for each document.
The example from above would look like this:
```
root
├── data
│   ├── images
│   │   ├── 1
│   │   │   ├── 1.jpg
│   │   │   ├── 1.json
│   │   ├── 2
│   │   │   ├── 2.jpg
│   │   │   ├── 2.json
│   │   ├── 3
│   │   │   ├── 3.jpg
│   │   │   ├── 3.json
│   │   ├── 4 
│   │   │   ├── 4.jpg
│   │   │   ├── 4.json
```
The `json` file should contain the `tags` of the `Document` in the following format:
```
{
    "price": 100,
    "brand": "Jina"
}
```




# Supported File Formats
Here is an overview of the supported file formats for each modality:

- Text: `.txt` (can also have a different extension, but has to be plain text)
- Image: `.jpg`, `.png`, ... (everything supported by `PIL`)
- Audio: `.wav`, `.mp3`, ... (everything supported by `librosa`)
- Video: `.gif` 