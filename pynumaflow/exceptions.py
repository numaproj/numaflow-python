class MarshalError(Exception):
    """Exception raised for errors for unsupported UDF Content-type.

    Attributes:
        udf_content_type -- the udf content type
    """

    def __init__(self, udf_content_type):
        self.udf_content_type = udf_content_type
        self.message = f"Unsupported UDF Content-type: {udf_content_type}"
        super().__init__(self.message)


class MissingHeaderError(Exception):
    pass


class InvalidContentTypeError(Exception):
    pass
