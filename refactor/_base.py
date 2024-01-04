from abc import ABCMeta


class NumaflowPythonUDF(metaclass=ABCMeta):
    """
    Base class for all Numaflow Python SDK based UDFs.

    Args:
        is_async: If True, the UDF is executed in an asynchronous manner.
        pl_conf: PipelineConf object
        _vtx: Vertex/UDF name
    """

    __slots__ = ("is_async", "pl_conf", "_vtx")

    def __init__(
        self,
        is_async: bool = False,
        pl_conf: Optional[PipelineConf] = None,
        _vtx: Optional[str] = "numalogic-udf",
    ):
        self._vtx = _vtx
        self.is_async = is_async
        self.pl_conf = pl_conf or PipelineConf()

    def __call__(
        self, keys: list[str], datum: Datum
    ) -> Union[Coroutine[None, None, Messages], Messages]:
        return self.aexec(keys, datum) if self.is_async else self.exec(keys, datum)
    #
    # # TODO: remove, and have an update config method
    # def register_conf(self, config_id: str, conf: StreamConf) -> None:
    #     """
    #     Register config with the UDF.
    #
    #     Args:
    #         config_id: Config ID
    #         conf: StreamConf object
    #     """
    #     self.pl_conf.stream_confs[config_id] = conf
    #
    # def _get_default_stream_conf(self, config_id) -> StreamConf:
    #     """Get the default config."""
    #     try:
    #         return self.pl_conf.stream_confs[_DEFAULT_CONF_ID]
    #     except KeyError:
    #         err_msg = f"Config with ID {config_id} or {_DEFAULT_CONF_ID} not found!"
    #         raise ConfigNotFoundError(err_msg) from None
    #
    # def _get_default_ml_pipeline_conf(self, config_id, pipeline_id) -> MLPipelineConf:
    #     """Get the default pipeline config."""
    #     try:
    #         return self.pl_conf.stream_confs[_DEFAULT_CONF_ID].ml_pipelines[_DEFAULT_CONF_ID]
    #     except KeyError:
    #         err_msg = (
    #             f"Pipeline with ID {pipeline_id} or {_DEFAULT_CONF_ID} "
    #             f"not found for config ID {config_id}!"
    #         )
    #         raise ConfigNotFoundError(err_msg) from None

    def exec(self, keys: list[str], datum: Datum) -> Messages:
        """
        Called when the UDF is executed in a synchronous manner.

        Args:
            keys: list of keys.
            datum: Datum object.

        Returns
        -------
            Messages instance
        """
        raise NotImplementedError("exec method not implemented")

    async def aexec(self, keys: list[str], datum: Datum) -> Messages:
        """
        Called when the UDF is executed in an asynchronous manner.

        Args:
            keys: list of keys.
            datum: Datum object.

        Returns
        -------
            Messages instance
        """
        raise NotImplementedError("aexec method not implemented")