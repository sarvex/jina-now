import io
from typing import Dict, List

import numpy as np
from docarray import Document
from now_common import options
from PIL import Image

from now.apps.base.app import JinaNOWApp
from now.constants import Apps, Modalities, Qualities


class TextToVideo(JinaNOWApp):
    def __init__(self):
        super().__init__()

    @property
    def app(self) -> str:
        return Apps.TEXT_TO_VIDEO

    @property
    def is_enabled(self) -> bool:
        return True

    @property
    def description(self) -> str:
        return 'Text to image search app'

    @property
    def input_modality(self) -> Modalities:
        return Modalities.TEXT

    @property
    def output_modality(self) -> Modalities:
        return Modalities.VIDEO

    @property
    def options(self) -> List[Dict]:
        return [options.QUALITY_CLIP]

    def set_flow_yaml(self, **kwargs):
        self.flow_yaml = 'flow-video-clip.yml'

    @property
    def pre_trained_embedding_size(self) -> Dict[Qualities, int]:
        return {
            Qualities.MEDIUM: 512,
            Qualities.GOOD: 512,
            Qualities.EXCELLENT: 768,
        }


def select_frames(num_selected_frames, num_total_frames):
    partition_size = num_total_frames / (num_selected_frames + 1)
    return [round(partition_size * (i + 1)) for i in range(num_selected_frames)]


def sample_video(d):
    video = d.blob
    video_io = io.BytesIO(video)
    gif = Image.open(video_io)
    frame_indices = select_frames(3, gif.n_frames)
    frames = []
    for i in frame_indices:
        gif.seek(i)
        frame = np.array(gif.convert("RGB"))
        frame_pil = Image.fromarray(frame)
        frame_pil_resized = frame_pil.resize((224, 224))
        frames.append(frame_pil_resized)
        frame_bytes = io.BytesIO()
        frame_pil_resized.save(frame_bytes, format="JPEG", quality=70)
        d.chunks.append(Document(blob=frame_bytes.getvalue()))
    small_gif_bytes_io = io.BytesIO()
    frames[0].save(
        small_gif_bytes_io,
        "gif",
        save_all=True,
        append_images=frames[1:],
        optimize=False,
        duration=300,
        loop=0,
        quality=75,
    )
    d.blob = small_gif_bytes_io.getvalue()
