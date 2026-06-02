from __future__ import annotations

import json
from typing import Any

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.ops.mapper.rpc_rate_limiter import RpcQpsRateLimiter, validate_qps
from data_juicer.utils.metrics_utils import emit_rpc_qps

OP_NAME = "item_id_image_url_mapper"


def _normalize_image_ref(
    value: Any,
    image_url_prefix: str | None = None,
    require_http_url: bool = False,
) -> str | None:
    if not value:
        return None

    value = str(value)
    if value.startswith(("http://", "https://")):
        return value
    if image_url_prefix:
        return f"{image_url_prefix.rstrip('/')}/{value.lstrip('/')}"
    if require_http_url:
        return None
    return value


@OPERATORS.register_module(OP_NAME)
class ItemIdImageUrlMapper(Mapper):
    """Resolve item image URLs from an item_id RPC and store them in one field."""

    def __init__(
        self,
        id_field: str = "item_id",
        output_url_field: str = "item_image_urls",
        image_url_prefix: str | None = None,
        require_http_url: bool = False,
        qps: int | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.
        :param id_field: input item id field used for RPC.
        :param output_url_field: field to store resolved item image URLs.
        :param image_url_prefix: optional prefix for non-HTTP image URI values.
        :param require_http_url: drop non-HTTP URI values when no prefix is set.
        :param qps: Ray job-level item image RPC request QPS limit.
        """
        validate_qps(qps)
        kwargs["image_key"] = output_url_field
        super().__init__(*args, **kwargs)
        self.id_field = id_field
        self.output_url_field = output_url_field
        self.image_url_prefix = image_url_prefix
        self.require_http_url = require_http_url
        self.qps = qps
        self._rpc_qps_limiter = RpcQpsRateLimiter(qps, self._rate_limiter_key())
        self._rpc_clients = None

    def __getstate__(self):
        state = dict(self.__dict__)
        state["_rpc_clients"] = None
        return state

    def prepare_backend_for_ray_tasks(self):
        self._rpc_qps_limiter.setup_ray_actor()

    def run(self, dataset, *, exporter=None, tracer=None):
        self.prepare_backend_for_ray_tasks()
        return super().run(dataset, exporter=exporter, tracer=tracer)

    def process_single(self, sample):
        urls = self._resolve_item_image_urls(sample.get(self.id_field))
        sample[self.output_url_field] = urls or []
        return sample

    def _resolve_item_image_urls(self, item_id: Any) -> list[str] | None:
        if item_id is None or str(item_id).strip() == "":
            return None

        rpc1, rpc2 = self._get_rpc_clients()
        for method, rpc in (("ItemImageInfoRPC", rpc2), ("ItemImageAttrRPC", rpc1)):
            try:
                self._rpc_qps_limiter.acquire()
                urls = rpc(item_id)
            except Exception:  # noqa: BLE001
                emit_rpc_qps(
                    op_name=self._name,
                    target="item_id_image_url",
                    method=method,
                    status="error",
                )
                urls = None
            else:
                emit_rpc_qps(
                    op_name=self._name,
                    target="item_id_image_url",
                    method=method,
                    status="success",
                )
            if urls:
                return list(urls)
        return None

    def _rate_limiter_key(self) -> str:
        return f"{OP_NAME}:item_id_image_url"

    def _get_rpc_clients(self):
        if self._rpc_clients is None:
            self._rpc_clients = self._create_rpc_clients()
        return self._rpc_clients

    def _create_rpc_clients(self):
        import euler

        euler.install_thrift_import_hook()
        from aigc_common.rpc.item_attr.item_attr import ItemAttrRPC
        from aigc_common.rpc.item_info.idl.ugc import item_info_service_thrift
        from aigc_common.rpc.item_info.item_info import ItemInfoRPC
        from harryspark.iudf import UDFMixin

        op = self

        class ItemImageAttrRPC(ItemAttrRPC, UDFMixin):
            def build_failed_result(self):
                return None

            def build_req(self, item_id: int):
                return item_info_service_thrift.GetItemDomainRequest(
                    Ids=[int(item_id)],
                    Fields=["UserId"],
                    Info={"with_deleted": "1"},
                )

            def process_resp(self, resp: Any, *args: Any, **kwargs: Any):
                base_resp = getattr(resp, "BaseResp", None)
                if base_resp is not None and getattr(base_resp, "StatusCode", 0) != 0:
                    return self.build_failed_result()

                item_attrs = getattr(resp, "ItemAttrList", None) or []
                if not item_attrs:
                    return self.build_failed_result()

                try:
                    item_attr_map = getattr(item_attrs[0], "ItemAttrMap", None)
                    item_attr = json.loads(item_attr_map) if isinstance(item_attr_map, str) else dict(item_attr_map)
                    original_images = item_attr.get("original_images", "[]")
                    image_infos = json.loads(original_images) if isinstance(original_images, str) else original_images
                except Exception:  # noqa: BLE001
                    return self.build_failed_result()

                image_urls = []
                for image_info in image_infos or []:
                    if not isinstance(image_info, dict):
                        continue
                    uri = _normalize_image_ref(
                        image_info.get("uri"),
                        op.image_url_prefix,
                        op.require_http_url,
                    )
                    if uri is not None:
                        image_urls.append((image_info.get("idx", 0), uri))
                image_urls.sort()
                return [uri for _, uri in image_urls] or None

        class ItemImageInfoRPC(ItemInfoRPC, UDFMixin):
            def build_failed_result(self):
                return None

            def build_req(self, item_id: int):
                return item_info_service_thrift.IdListRequest(
                    Ids=[int(item_id)],
                    Info={"stats": "0"},
                )

            def process_resp(self, resp: Any, item_id: int | None = None):
                base_resp = getattr(resp, "BaseResp", None)
                if base_resp is not None and getattr(base_resp, "StatusCode", 0) != 0:
                    return self.build_failed_result()

                items = getattr(resp, "Items", None) or []
                if not items:
                    return self.build_failed_result()

                try:
                    content_dict = json.loads(items[0].Content)
                except Exception:  # noqa: BLE001
                    return self.build_failed_result()

                image_urls = []
                for image_info in content_dict.get("images", []) or []:
                    if not isinstance(image_info, dict):
                        continue
                    uri = _normalize_image_ref(
                        image_info.get("uri"),
                        op.image_url_prefix,
                        op.require_http_url,
                    )
                    if uri is not None:
                        image_urls.append(uri)
                return image_urls or None

        return ItemImageAttrRPC(), ItemImageInfoRPC()
