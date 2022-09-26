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

NOW gives the world access to neural image search in just one command execution.
Main features
- ⛅ **Cloud**: We take care of the deployment and maintenance
- 🐥 **Easy**: Minimal effort required to set up your search case
- 🐎 **Fast**: Set up your search case within minutes
- 🌈 **Quality**: If you provide labels to your documents, Jina NOW fine-tunes a model for you
- ✨ **Nocode**: Deployment can be done by non-technical people

You can read more on how Jina NOW is production ready [here](https://now.jina.ai/user-guides/production/).

### Installation

```bash
pip install jina-now
```

In case you need sudo for running Docker, install and use jina-now using sudo as well.

#### Mac M1

For the Mac M1 it is generally recommended using a conda environment as outlined in the [Jina documentation](https://docs.jina.ai/get-started/install/troubleshooting/#on-mac-m1).
In a new conda environment first execute `conda install grpcio tokenizers protobuf`. Then run `pip install jina-now`.

### Usage
You can use the following command to start Jina NOW.
```bash
jina now start
```
First, you will get asked what search case you would like to deploy. 


### Quick Start
```bash
jina now start
```
First, you will be prompted to choose an app. As for now, we support images or text searches. But in the future, we will add many more options here.

<img width="613" alt="Screenshot 2022-05-31 at 01 08 25" src="https://user-images.githubusercontent.com/40893766/192245825-c19274a5-2514-4c93-94ff-4d507dfef429.png">

In the next step, you get asked to select the dataset for your search app. You could either choose one of our existing datasets or select `custom` to index your own data.

<img width="422" alt="question-ds" src="https://user-images.githubusercontent.com/11627845/170263852-46776391-a906-417c-8528-e1fb7058c33a.png">

When choosing `custom`, you can decide in what format you provide your data. The recommended way, is to push a document array described [here](https://docarray.jina.ai/fundamentals/documentarray/serialization/#from-to-cloud).
Alternatively, you can specify a URL where a document array can be downloaded from.
Also, it is possible to provide a local folder where the Images are located. In case of text search it would be a local text file.

<img width="724" alt="question-custom" src="https://user-images.githubusercontent.com/40893766/192245979-66376fc2-2629-4932-a747-b3f526bfd53a.png">

If you chose `docarray.pull`, you will be asked to insert your docarray id. 
Likewise, if you chose docarray URL, you will be prompted to enter the URL.
In case you selected local path, `jina-now` will ask you to enter the local path of the data folder as shown bellow.

<img width="506" alt="question-local-path" src="https://user-images.githubusercontent.com/11627845/170256044-67e82e86-6439-4a3e-98f1-dbdf1940de67.png">

Currently, we provide two deployment options. We recommend using the cloud deployment. This will run your search app on our servers.
Alternatively, you can select the local deployment option.

<img width="547" alt="question-deployment" src="https://user-images.githubusercontent.com/11627845/170256038-8c44a5b8-985a-4fe7-af5d-16df0244f4bb.png">

In case of local deployment, you will be asked where you want to deploy it. Jina NOW reads your local .kube/config and lists all kubernetes clusters you have access to.
If you don't want to use an existing cluster, you can create a new one locally.

<img width="643" alt="question-cluster" src="https://user-images.githubusercontent.com/11627845/170256027-99798fae-3ec4-42dc-8737-843f4a23f941.png">

After the program execution is finished, two links will be shown to you. The first one brings you to the
swagger UI which is useful for frontend integration.
The second URL leads you to a playground where you can run example queries and experiment with the search case.

<img width="709" alt="Screenshot 2022-05-26 at 16 34 56" src="https://user-images.githubusercontent.com/11627845/170511632-c741a418-1246-4c23-aadd-cfd74d783f6b.png">

Example of the playground.

<img width="350" alt="Screenshot 2022-05-26 at 16 36 49" src="https://user-images.githubusercontent.com/11627845/170511607-3fb810f7-a5aa-47cd-9f70-e6034a96b9fd.png">

Example of the swagger ui.

<img width="350" alt="Screenshot 2022-05-26 at 16 36 06" src="https://user-images.githubusercontent.com/11627845/170511580-230d1e41-5e14-4623-adb6-3d4b2d400dc9.png">

For more information on how to use Jina NOW CLI and API click [here](https://now.jina.ai/user-guides/cli_api/)

## Supported apps (more will be added)

- [x] Text to Image search 📝 ▶ 🏞 
- [x] Image to Text search 🏞 ▶ 📝 
- [x] Image to Image search 🏞 ▶ 🏞
- [x] Text to Text search 📝 ▶ 📝
- [x] Music to Music search 🥁 ▶ 🥁 
- [x] Text to Video search 📝 ▶ 🎥 (only gif at the moment)
- [ ] Text to 3D Mesh search 📝 ▶ 🧊
- [ ] ...

[![IMAGE ALT TEXT HERE](https://user-images.githubusercontent.com/11627845/164571632-0e6a6c39-0137-413b-8287-21fc34785665.png)](https://www.youtube.com/watch?v=fdIaLP0ctpo)
</p>
<br>
  
## Examples

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


### Now use your custom data :)
<!-- end elevator-pitch -->

