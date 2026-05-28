import asyncio
from collections import Counter
import copy
import os
import os.path as osp
import time
from typing import List, Union
from urllib.parse import urlparse

import aiohttp
from loguru import logger

from data_juicer.utils.file_utils import download_file, is_remote_path
from data_juicer.utils.metrics_utils import (
    emit_download_bytes,
    emit_download_latency_ms,
    emit_download_qps,
)

from ...base_op import OPERATORS, Mapper

OP_NAME = "download_file_mapper"


@OPERATORS.register_module(OP_NAME)
class DownloadFileMapper(Mapper):
    """Mapper to download URL files to local files or load them into memory.

    This operator downloads files from URLs and can either save them to a specified
    directory or load the contents directly into memory. It supports downloading multiple
    files concurrently and can resume downloads if the `resume_download` flag is set. The
    operator processes nested lists of URLs, flattening them for batch processing and then
    reconstructing the original structure in the output. If both `save_dir` and `save_field`
    are not specified, it defaults to saving the content under the key `image_bytes`. The
    operator logs any failed download attempts and provides error messages for
    troubleshooting."""

    _batched_op = True
    _FAILED_DOWNLOAD_LOG_INTERVAL = 100

    def __init__(
        self,
        download_field: str = None,
        save_dir: str = None,
        save_field: str = None,
        resume_download: bool = False,
        timeout: int = 30,
        retry_times: int = 1,
        max_concurrent: int = 10,
        filter_non_url: bool = False,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param save_dir: The directory to save downloaded files.
        :param download_field: The filed name to get the url to download.
        :param save_field: The filed name to save the downloaded file content.
        :param resume_download: Whether to resume download. if True, skip the sample if it exists.
        :param timeout: Timeout for download.
        :param retry_times: Maximum attempts for each remote download.
        :param max_concurrent: Maximum concurrent downloads.
        :param filter_non_url: Whether to drop values that are not remote URLs
            before download. Local file paths are preserved when this is False.
        :param args: extra args
        :param kwargs: extra args
        """
        super().__init__(*args, **kwargs)
        self._init_parameters = self.remove_extra_parameters(locals())

        self.download_field = download_field
        self.save_dir = save_dir
        self.save_field = save_field
        self.resume_download = resume_download
        if not (self.save_dir or self.save_field):
            logger.warning(
                "Both `save_dir` and `save_field` are not specified. Use the default `image_bytes` key to "
                "save the downloaded contents."
            )
            self.save_field = self.image_bytes_key
        if self.save_dir:
            os.makedirs(self.save_dir, exist_ok=True)
        self.timeout = timeout
        self.retry_times = max(1, retry_times)
        self.max_concurrent = max_concurrent
        self.filter_non_url = filter_non_url
        self._failed_download_count = 0
        self._failed_download_log_bucket = 0
        self._failed_download_summary = Counter()

    def download_files_async(self, urls, return_contents, save_dir=None, **kwargs):

        async def _download_file(
            session: aiohttp.ClientSession,
            semaphore: asyncio.Semaphore,
            idx: int,
            url: str,
            save_dir=None,
            return_content=False,
            **kwargs,
        ) -> dict:
            started_at = time.monotonic()
            status, response, content, save_path = "success", None, None, None
            try:
                # local file
                if not is_remote_path(url):
                    if return_content:
                        with open(url, "rb") as f:
                            content = f.read()
                    if save_dir:
                        save_path = url
                    return idx, save_path, status, response, content

                # skip already downloaded files
                if not save_dir and not return_content:
                    return idx, save_path, status, response, content

                if save_dir:
                    filename = os.path.basename(urlparse(url).path)
                    save_path = osp.join(save_dir, filename)
                    if os.path.exists(save_path):
                        if return_content:
                            with open(save_path, "rb") as f:
                                content = f.read()
                        return idx, save_path, status, response, content

                async with semaphore:
                    last_error = None
                    for _ in range(self.retry_times):
                        try:
                            response, content = await download_file(
                                session, url, save_path, return_content=True, timeout=self.timeout, **kwargs
                            )
                            last_error = None
                            break
                        except Exception as e:
                            last_error = e
                    if last_error is not None:
                        raise last_error
            except Exception as e:
                status = "failed"
                response = str(e)
                save_path = None
                content = None
            finally:
                self._emit_download_metrics(
                    url=url,
                    status=status,
                    save_dir=save_dir,
                    return_content=return_content,
                    content=content,
                    save_path=save_path,
                    started_at=started_at,
                )

            return idx, save_path, status, response, content

        async def run_downloads(urls, return_contents, save_dir=None, **kwargs):
            semaphore = asyncio.Semaphore(self.max_concurrent)
            async with aiohttp.ClientSession() as session:
                tasks = [
                    _download_file(session, semaphore, idx, url, save_dir, return_contents[idx], **kwargs)
                    for idx, url in enumerate(urls)
                ]
                return await asyncio.gather(*tasks)

        results = asyncio.run(run_downloads(urls, return_contents, save_dir, **kwargs))
        results.sort(key=lambda x: x[0])

        return results

    def _flat_urls(self, nested_urls):
        flat_urls = []
        structure_info = []  # save as original index, sub index

        for idx, urls in enumerate(nested_urls):
            if isinstance(urls, list):
                for sub_idx, url in enumerate(urls):
                    flat_urls.append(url)
                    structure_info.append((idx, sub_idx))
            else:
                flat_urls.append(urls)
                structure_info.append((idx, -1))  # -1 means single str element

        return flat_urls, structure_info

    def _filter_nested_urls(self, nested_urls):
        if not self.filter_non_url:
            return nested_urls

        filtered = []
        for urls in nested_urls:
            if isinstance(urls, list):
                filtered.append([url.strip() for url in urls if self._is_remote_url(url)])
            elif self._is_remote_url(urls):
                filtered.append(urls.strip())
            else:
                filtered.append([])
        return filtered

    @staticmethod
    def _is_remote_url(url):
        return isinstance(url, str) and is_remote_path(url.strip())

    def _emit_download_metrics(
        self,
        *,
        url,
        status: str,
        save_dir,
        return_content: bool,
        content,
        save_path,
        started_at: float,
    ) -> None:
        scheme = self._download_scheme(url)
        save_mode = self._download_save_mode(save_dir, return_content)
        latency_ms = max(0.0, (time.monotonic() - started_at) * 1000.0)
        emit_download_qps(
            op_name=self._name,
            scheme=scheme,
            status=status,
            save_mode=save_mode,
        )
        byte_count = self._download_byte_count(content, save_path)
        if status == "success" and byte_count is not None:
            emit_download_bytes(
                op_name=self._name,
                scheme=scheme,
                byte_count=byte_count,
                save_mode=save_mode,
            )
        emit_download_latency_ms(
            op_name=self._name,
            scheme=scheme,
            status=status,
            latency_ms=latency_ms,
            save_mode=save_mode,
        )

    @staticmethod
    def _download_scheme(url) -> str:
        if not isinstance(url, str):
            return "unknown"
        scheme = urlparse(url.strip()).scheme.lower()
        return scheme or "file"

    @staticmethod
    def _download_save_mode(save_dir, return_content: bool) -> str:
        if save_dir and return_content:
            return "file_and_memory"
        if save_dir:
            return "file"
        if return_content:
            return "memory"
        return "noop"

    @staticmethod
    def _download_byte_count(content, save_path) -> int | None:
        if content is not None:
            try:
                return len(content)
            except TypeError:
                return None
        if save_path and osp.isfile(save_path):
            try:
                return osp.getsize(save_path)
            except OSError:
                return None
        return None

    def _create_path_struct(self, nested_urls, keep_failed_url=True) -> str:
        if keep_failed_url:
            reconstructed = copy.deepcopy(nested_urls)
        else:
            reconstructed = []
            for item in nested_urls:
                if isinstance(item, list):
                    reconstructed.append([None] * len(item))
                else:
                    reconstructed.append(None)

        return reconstructed

    def _create_save_field_struct(self, nested_urls, save_field_contents=None) -> str:
        if save_field_contents is None:
            save_field_contents = []
            for item in nested_urls:
                if isinstance(item, list):
                    save_field_contents.append([None] * len(item))
                else:
                    save_field_contents.append(None)
        else:
            # check whether the save_field_contents format is correct and correct it automatically
            for i, item in enumerate(nested_urls):
                if isinstance(item, list):
                    if not save_field_contents[i] or len(save_field_contents[i]) != len(item):
                        save_field_contents[i] = [None] * len(item)

        return save_field_contents

    @staticmethod
    def _summarize_failed_download_response(response):
        summary = str(response).replace("\n", " ").strip()
        if ", url=" in summary:
            summary = summary.split(", url=", 1)[0]
        elif " url=" in summary:
            summary = summary.split(" url=", 1)[0]
        if len(summary) > 300:
            summary = summary[:297] + "..."
        return summary or "<empty error>"

    def _record_failed_downloads(self, failed_count, failed_summary):
        if failed_count <= 0:
            return

        self._failed_download_count += failed_count
        self._failed_download_summary.update(failed_summary)
        log_bucket = self._failed_download_count // self._FAILED_DOWNLOAD_LOG_INTERVAL
        if log_bucket <= self._failed_download_log_bucket:
            return

        top_errors = "; ".join(
            f"{count} x {error}" for error, count in self._failed_download_summary.most_common(5)
        )
        while self._failed_download_log_bucket < log_bucket:
            self._failed_download_log_bucket += 1
            logger.error(
                f"download_file_mapper failures reached "
                f"{self._failed_download_log_bucket * self._FAILED_DOWNLOAD_LOG_INTERVAL} in this worker "
                f"({self._failed_download_count} total). Top errors: {top_errors}"
            )

    def download_nested_urls(self, nested_urls: List[Union[str, List[str]]], save_dir=None, save_field_contents=None):
        flat_urls, structure_info = self._flat_urls(nested_urls)
        if not flat_urls:
            reconstructed_path = self._create_path_struct(nested_urls) if self.save_dir else None
            return save_field_contents, reconstructed_path, 0, Counter()

        if save_field_contents is None:
            # not save contents, set return_contents to False
            return_contents = [False] * len(flat_urls)
        else:
            # if original content None, set bool value to True to get content else False to skip reload it
            return_contents = []
            for contents in save_field_contents:
                if isinstance(contents, list):
                    return_contents.extend(not content for content in contents)
                else:
                    return_contents.append(not contents)

        download_results = self.download_files_async(
            flat_urls,
            return_contents,
            save_dir,
        )

        if self.save_dir:
            reconstructed_path = self._create_path_struct(nested_urls)
        else:
            reconstructed_path = None

        failed_count = 0
        failed_summary = Counter()
        for i, (idx, save_path, status, response, content) in enumerate(download_results):
            orig_idx, sub_idx = structure_info[i]
            if status != "success":
                save_path = flat_urls[i]
                failed_count += 1
                failed_summary[self._summarize_failed_download_response(response)] += 1

            if save_field_contents is not None:
                if return_contents[i]:
                    if sub_idx == -1:
                        save_field_contents[orig_idx] = content
                    else:
                        save_field_contents[orig_idx][sub_idx] = content

            if self.save_dir:
                # TODO: add download stats
                if sub_idx == -1:
                    reconstructed_path[orig_idx] = save_path
                else:
                    reconstructed_path[orig_idx][sub_idx] = save_path

        return save_field_contents, reconstructed_path, failed_count, failed_summary

    def process_batched(self, samples):
        if self.download_field not in samples or not samples[self.download_field]:
            return samples

        batch_nested_urls = self._filter_nested_urls(samples[self.download_field])
        if self.filter_non_url:
            samples[self.download_field] = batch_nested_urls

        if self.save_field:
            if not self.resume_download:
                if self.save_field in samples:
                    raise ValueError(
                        f"{self.save_field} is already in samples. '\
                        'If you want to resume download, please set `resume_download=True`"
                    )
                save_field_contents = self._create_save_field_struct(batch_nested_urls)
            else:
                if self.save_field not in samples:
                    save_field_contents = self._create_save_field_struct(batch_nested_urls)
                else:
                    save_field_contents = self._create_save_field_struct(batch_nested_urls, samples[self.save_field])
        else:
            save_field_contents = None

        save_field_contents, reconstructed_path, failed_count, failed_summary = self.download_nested_urls(
            batch_nested_urls, save_dir=self.save_dir, save_field_contents=save_field_contents
        )

        if self.save_dir:
            samples[self.download_field] = reconstructed_path

        if self.save_field:
            samples[self.save_field] = save_field_contents

        self._record_failed_downloads(failed_count, failed_summary)

        return samples
