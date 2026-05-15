from __future__ import annotations

from typing import Any

from data_juicer.ops.base_op import OPERATORS, Mapper

OP_NAME = "prepare_operator_context_mapper"
CTX_FIELD = "ctx"


@OPERATORS.register_module(OP_NAME)
class PrepareOperatorContextMapper(Mapper):
    """Prepare shared operator context fields for downstream business mappers."""

    def __init__(
        self,
        user_account: str | None = None,
        tt_env: str | None = None,
        use_ppe: str | None = None,
        overwrite: bool = False,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param user_account: account used by notification and downstream context.
        :param tt_env: optional PPE environment header value.
        :param use_ppe: optional PPE switch header value.
        :param overwrite: whether to overwrite existing ctx values.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not user_account:
            raise ValueError("user_account must be provided")

        self.context_values = {
            "userAccount": str(user_account),
        }
        if tt_env:
            self.context_values["x-tt-env"] = str(tt_env)
        if use_ppe:
            self.context_values["x-use-ppe"] = str(use_ppe)
        self.overwrite = overwrite

    def process_single(self, sample: dict[str, Any]):
        ctx = sample.get(CTX_FIELD)
        if not isinstance(ctx, dict):
            ctx = {}
            sample[CTX_FIELD] = ctx

        for key, value in self.context_values.items():
            if self.overwrite or key not in ctx:
                ctx[key] = value
        return sample
