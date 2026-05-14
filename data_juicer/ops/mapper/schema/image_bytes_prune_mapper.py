from io import BytesIO
from typing import Optional

import numpy as np
from PIL import Image

from data_juicer.ops.base_op import OPERATORS, Mapper

OP_NAME = "image_bytes_prune_mapper"


@OPERATORS.register_module(OP_NAME)
class ImageBytesPruneMapper(Mapper):
    """Prune invalid images from aligned URL and image-bytes lists within a sample."""

    def __init__(
        self,
        image_key: str = "images",
        image_bytes_key: str = "image_bytes",
        valid_image_count_key: str = "valid_image_count",
        min_image_area: int = 100 * 100,
        max_image_area: int = 3000 * 3000,
        max_aspect_ratio: float = 20,
        max_gray_bucket_ratio: float = 0.95,
        *args,
        **kwargs,
    ):
        """
        Initialization method.
        :param image_key: field that stores image URLs.
        :param image_bytes_key: field that stores downloaded image bytes.
        :param valid_image_count_key: field to store the remaining image count.
        :param min_image_area: minimum accepted image area in pixels.
        :param max_image_area: maximum accepted image area in pixels.
        :param max_aspect_ratio: maximum accepted width/height or height/width ratio.
        :param max_gray_bucket_ratio: maximum accepted grayscale histogram bucket ratio.
        :param args: extra args.
        :param kwargs: extra args.
        """
        kwargs["image_key"] = image_key
        kwargs["image_bytes_key"] = image_bytes_key
        super().__init__(*args, **kwargs)
        self.valid_image_count_key = valid_image_count_key
        self.min_image_area = min_image_area
        self.max_image_area = max_image_area
        self.max_aspect_ratio = max_aspect_ratio
        self.max_gray_bucket_ratio = max_gray_bucket_ratio

    def process_single(self, sample):
        urls = self._as_list(sample.get(self.image_key))
        image_bytes = self._as_list(sample.get(self.image_bytes_key))
        kept_urls = []
        kept_bytes = []

        for url, img_bytes in zip(urls, image_bytes):
            if self.is_valid_image_bytes(img_bytes):
                kept_urls.append(url)
                kept_bytes.append(img_bytes)

        sample[self.image_key] = kept_urls
        sample[self.image_bytes_key] = kept_bytes
        sample[self.valid_image_count_key] = len(kept_bytes)
        return sample

    @staticmethod
    def _as_list(value):
        if value is None:
            return []
        return value if isinstance(value, list) else [value]

    def is_valid_image_bytes(self, image_bytes: Optional[bytes]) -> bool:
        if not image_bytes:
            return False

        try:
            img = Image.open(BytesIO(image_bytes))
            img = self._to_rgb(img)
            width, height = img.size
            area = width * height
            if area < self.min_image_area or area > self.max_image_area:
                return False
            if width / height > self.max_aspect_ratio or height / width > self.max_aspect_ratio:
                return False

            img_gray = np.array(img).mean(-1)
            histogram, _ = np.histogram(img_gray, bins=256 // 10)
            ratios = histogram / np.sum(histogram)
            if np.max(ratios) > self.max_gray_bucket_ratio:
                return False
        except Exception:
            return False

        return True

    @staticmethod
    def _to_rgb(pil_image: Image.Image) -> Image.Image:
        if pil_image.mode == "RGBA":
            white_background = Image.new("RGB", pil_image.size, (255, 255, 255))
            white_background.paste(pil_image, mask=pil_image.split()[3])
            return white_background
        return pil_image.convert("RGB")
