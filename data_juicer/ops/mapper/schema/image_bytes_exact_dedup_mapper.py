import hashlib

from data_juicer.ops.base_op import OPERATORS, Mapper

OP_NAME = "image_bytes_exact_dedup_mapper"


@OPERATORS.register_module(OP_NAME)
class ImageBytesExactDedupMapper(Mapper):
    """Deduplicate aligned image URL and bytes lists within each sample by exact bytes."""

    def __init__(
        self,
        image_key: str = "images",
        image_bytes_key: str = "image_bytes",
        md5_key: str = "md5",
        valid_image_count_key: str = "valid_image_count",
        preserve_existing_md5_on_empty: bool = False,
        *args,
        **kwargs,
    ):
        """
        Initialization method.
        :param image_key: field that stores image URLs.
        :param image_bytes_key: field that stores image bytes.
        :param md5_key: field to store the sample-level md5.
        :param valid_image_count_key: field to store the remaining image count.
        :param preserve_existing_md5_on_empty: keep the existing md5 when no
            image bytes remain.
        :param args: extra args.
        :param kwargs: extra args.
        """
        kwargs["image_key"] = image_key
        kwargs["image_bytes_key"] = image_bytes_key
        super().__init__(*args, **kwargs)
        self.md5_key = md5_key
        self.valid_image_count_key = valid_image_count_key
        self.preserve_existing_md5_on_empty = preserve_existing_md5_on_empty

    def process_single(self, sample):
        urls = self._as_list(sample.get(self.image_key))
        image_bytes = self._as_list(sample.get(self.image_bytes_key))
        if not image_bytes:
            sample[self.image_key] = []
            sample[self.image_bytes_key] = []
            sample[self.valid_image_count_key] = 0
            if not self.preserve_existing_md5_on_empty:
                sample[self.md5_key] = hashlib.md5().hexdigest()
            return sample

        bytes_urls = sorted(zip(image_bytes, urls), key=lambda item: item[0])

        deduped_bytes = []
        deduped_urls = []
        seen = set()
        sample_md5 = hashlib.md5()
        for img_bytes, url in bytes_urls:
            img_md5 = hashlib.md5(img_bytes).hexdigest()
            if img_md5 in seen:
                continue
            seen.add(img_md5)
            deduped_bytes.append(img_bytes)
            deduped_urls.append(url)
            sample_md5.update(img_bytes)

        sample[self.image_key] = deduped_urls
        sample[self.image_bytes_key] = deduped_bytes
        sample[self.valid_image_count_key] = len(deduped_bytes)
        sample[self.md5_key] = sample_md5.hexdigest()
        return sample

    @staticmethod
    def _as_list(value):
        if value is None:
            return []
        return value if isinstance(value, list) else [value]
