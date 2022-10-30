from fastapi import Request
from fastapi.responses import JSONResponse


class InvalidWorld(Exception):
    def __init__(self):
        self.msg = "invalid world"


class RateLimitExceeded(Exception):
    def __init__(self):
        self.msg = "ratelimit exceeded"


class InvalidArgument(Exception):
    def __init__(self, argument_name, argument):
        self.msg = f"invalid argument for {argument_name}: {argument}"


def initiate_errors(app):
    for exception in (InvalidWorld, RateLimitExceeded, InvalidArgument):
        @app.exception_handler(exception)
        async def unicorn_exception_handler(_, exc: exception):
            return JSONResponse(status_code=420, content={"message": exc.msg})
