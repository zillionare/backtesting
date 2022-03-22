import logging
from enum import Enum
from functools import wraps
from typing import Any, Union

from expiringdict import ExpiringDict
from sanic import Sanic, response

seen_requests = ExpiringDict(max_len=1000, max_age_seconds=10 * 60)

logger = logging.getLogger(__name__)


def get_app_context():
    app = Sanic.get_app("backtest")
    return app.ctx


def check_token(request):
    if not request.token:
        return False

    app = Sanic.get_app("backtest")

    if app.ctx.accounts.is_valid(request.token):
        request.ctx.broker = app.ctx.accounts.get_broker(request.token)
        return True
    else:
        return False


def check_duplicated_request(request):
    request_id = request.headers.get("Request-ID")
    if request_id in seen_requests:
        return True

    seen_requests[request_id] = True
    request.ctx.request_id = request_id
    return False


def protected(wrapped):
    """check token and duplicated request"""

    def decorator(f):
        @wraps(f)
        async def decorated_function(request, *args, **kwargs):
            is_authenticated = check_token(request)
            is_duplicated = check_duplicated_request(request)

            if is_authenticated and not is_duplicated:
                try:
                    result = await f(request, *args, **kwargs)
                    return result
                except Exception as e:
                    logger.exception(e)
                    return response.text(str(e), status=500)
            elif not is_authenticated:
                return response.json(
                    {
                        "msg": "token is invalid",
                    },
                    401,
                )
            elif is_duplicated:
                return response.json(
                    {
                        "msg": "duplicated request",
                    },
                    200,
                )

        return decorated_function

    return decorator(wrapped)


def make_response(err_code: Union[Enum, int], data: Any = None, err_msg: str = None):
    if err_msg is None:
        err_msg = str(err_code)

    return {
        "status": err_code.value if isinstance(err_code, Enum) else err_code,
        "msg": err_msg,
        "data": data,
    }
