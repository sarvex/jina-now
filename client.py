from docarray import Document, dataclass, field
from docarray.typing import Image, Text, Video
from jina import Client

client = Client(host="grpcs://nowapi-7f1f885e71-grpc.wolf.jina.ai")


@dataclass
class MMQueryDoc:
    query_text: Text = field(default=None)  # text, uri
    query_image: Image = field(default=None)  # blob, uri, tensor
    query_video: Video = field(default=None)  # blob, uri, tensor


doc = Document(MMQueryDoc(query_text='hello'))

search = client.post(
    on='/search',
    inputs=doc,
)

# suggestion = client.post(
#     on='/search',
#     inputs=Document(text='h'),
# )

suggestion[0].summary()
print(suggestion[0].tags['suggestions'])
