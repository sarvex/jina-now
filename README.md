<p align="center">

<img src="https://github.com/jina-ai/now/blob/main/docs/_static/logo-light.svg?raw=true" alt="Jina NOW logo: The data structure for unstructured data" width="300px">  


<br>
One command to host them all. Bring your search case into the cloud in minutes. <br>
Tell us what you think: <a href="https://10sw1tcpld4.typeform.com/to/VTAyYRpR?utm_source=cli">Survey</a>
</p>

<p align=center>
<a href="https://pypi.org/project/jina-now/"><img src="https://github.com/jina-ai/jina/blob/master/.github/badges/python-badge.svg?raw=true" alt="Python 3.7 3.8 3.9 3.10" title="Jina NOW supports Python 3.7 and above"></a>
<a href="https://pypi.org/project/jina-now/"><img src="https://img.shields.io/pypi/v/jina-now?color=%23099cec&amp;label=PyPI&amp;logo=pypi&amp;logoColor=white" alt="PyPI"></a>
</p>


<p align="center">
<img src="https://user-images.githubusercontent.com/11627845/164569398-5ef22a41-e2e1-438a-88a5-2ac43ad9426d.gif" alt="Jina NOW logo: The data structure for unstructured data" width="600px">

<!-- start elevator-pitch -->

NOW gives the world access to neural image search with just one command.
 
- ⛅ **Cloud**: We handle deployment and maintenance.
- 🐎 **Fast and easy**: Set up your search use case in minutes with minimal effort.
- 🌈 **Quality**: You provide the labels, NOW fine-tunes the model.
- ✨ **Nocode**: Non-technical people can deploy with ease.

Read how [Jina NOW is production ready](https://now.jina.ai/user-guides/production/).

### Install

```bash
pip install jina-now
```

If you need sudo to run Docker, use sudo to install and use Jina NOW as well.

#### Mac M1

For the M1 we recommend using a [Conda environment](https://docs.jina.ai/get-started/install/troubleshooting/#on-mac-m1).
In a new Conda environment:

1. Run `conda install grpcio tokenizers protobuf`
2. Run `pip install jina-now`.

### Quick start
```bash
jina now start
```
**1.** Choose your app. Currently, we support image or text search, but in future, we'll add many more options.

<img width="613" alt="Screenshot 2022-05-31 at 01 08 25" src="https://user-images.githubusercontent.com/40893766/192245825-c19274a5-2514-4c93-94ff-4d507dfef429.png">

**2.** Choose your data source. Choose between demo datasets, a custom DocumentArray, local folder, or S3 bucket.

<img src="https://user-images.githubusercontent.com/47435119/195334616-9a8d5ad3-229f-49bb-9d49-f38243a0eb9b.png">

- If you choose `docarray.pull` or DocumentArray URL, NOW asks for your DocumentArray ID or URL.
- If you choose local path, NOW asks for the folder's path:

<img width="506" alt="question-local-path" src="https://user-images.githubusercontent.com/11627845/170256044-67e82e86-6439-4a3e-98f1-dbdf1940de67.png">

**3.** Choose your deployment type. We recommend cloud deployment to run your app on our servers. Alternatively, you can deploy locally.

<img width="547" alt="question-deployment" src="https://user-images.githubusercontent.com/11627845/170256038-8c44a5b8-985a-4fe7-af5d-16df0244f4bb.png">

For local deployment, NOW asks where you want to deploy it and reads your local .kube/config and lists all Kubernetes clusters you have access to.
If you don't want to use an existing cluster, you can create a new one locally.

<img width="643" alt="question-cluster" src="https://user-images.githubusercontent.com/11627845/170256027-99798fae-3ec4-42dc-8737-843f4a23f941.png">

**4.** Follow the links. After NOW finishes processing, you'll see two links:

- The Swagger UI is useful for frontend integration.
- The "playground" lets you run example queries and experiment with your search use case.

<img width="709" alt="Screenshot 2022-05-26 at 16 34 56" src="https://user-images.githubusercontent.com/11627845/170511632-c741a418-1246-4c23-aadd-cfd74d783f6b.png">

Example of the playground.

<img width="350" alt="Screenshot 2022-05-26 at 16 36 49" src="https://user-images.githubusercontent.com/11627845/170511607-3fb810f7-a5aa-47cd-9f70-e6034a96b9fd.png">

Example of the Swagger UI.

<img width="350" alt="Screenshot 2022-05-26 at 16 36 06" src="https://user-images.githubusercontent.com/11627845/170511580-230d1e41-5e14-4623-adb6-3d4b2d400dc9.png">

[More information on using Jina NOW CLI and API](https://now.jina.ai/user-guides/cli_api/)

## Supported apps (more coming soon)

- 📝 ▶ 🏞 Text to Image search 
- 🏞 ▶ 📝 Image to Text search 
- 🏞 ▶ 🏞 Image to Image search 
- 📝 ▶ 📝 Text to Text search 
- 🥁 ▶ 🥁 Music to Music search 
- 📝 ▶ 🎥 Text to Video search (for GIFs)
- 📝 ▶ 🧊 Text to 3D Mesh search (coming soon)

[![](https://user-images.githubusercontent.com/11627845/164571632-0e6a6c39-0137-413b-8287-21fc34785665.png)](https://www.youtube.com/watch?v=fdIaLP0ctpo)
</p>
  
## Example datasets

<details><summary>👕 Fashion</summary>
<img width="400" alt="image" src="https://user-images.githubusercontent.com/11627845/157079335-8f36fc73-d826-4c0a-b1f3-ed5d650a1af1.png">
</details>

<details><summary>☢️ Chest X-Ray</summary>
<img src="https://user-images.githubusercontent.com/11627845/157067695-59851a77-5c43-4f68-80c4-403fec850776.png" width="400">
</details>
  
<details><summary>💰 NFT - bored apes</summary>
<img src="https://user-images.githubusercontent.com/11627845/157019002-573cc101-e23b-4020-825c-f37ec66c6ccf.jpeg" width="400">
</details>
  
<details><summary>🖼 Art</summary>
<img width="400" alt="image" src="https://user-images.githubusercontent.com/11627845/157074453-721c0f2d-3f7d-4839-b6ff-bbccbdba2e5f.png">
</details>
  
<details><summary>🚗 Cars</summary>
<img width="400" alt="image" src="https://user-images.githubusercontent.com/11627845/157081047-792df6bd-544d-420c-b180-df824c802e73.png">
</details>
  
<details><summary>🏞 Street view</summary>
<img width="400" alt="image" src="https://user-images.githubusercontent.com/11627845/157087532-46ae36a2-c97f-45d7-9c3e-c624dcf6dc46.png">
</details>

<details><summary>🦆 Birds</summary>
<img width="400" alt="image" src="https://user-images.githubusercontent.com/11627845/157069954-615a5cb6-dda0-4a2f-9442-ea807ad4a8d5.png">
</details>

<!-- end elevator-pitch -->

