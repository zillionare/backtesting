from enum import Enum
from functools import wraps
from typing import Dict, List, Union

from expiringdict import ExpiringDict
from sanic import Sanic, response

seen_requests = ExpiringDict(max_len=1000, max_age_seconds=10 * 60)


def get_app_context():
    app = Sanic.get_app("backtest")
    return app.ctx


def check_token(request):
    if not request.token:
        return False

    app = Sanic.get_app("backtest")
    if request.token != app.ctx.cfg.account.token:
        return False

    return True


def check_duplicated_request(request):
    request_id = request.headers.get("Request-ID")
    if request_id in seen_requests:
        return False

    seen_requests[request_id] = True
    return True


def protected(wrapped):
    """check token and duplicated request"""

    def decorator(f):
        @wraps(f)
        async def decorated_function(request, *args, **kwargs):
            is_authenticated = check_token(request)
            not_duplicated = check_duplicated_request(request)

            if is_authenticated and not_duplicated:
                result = await f(request, *args, **kwargs)
                return result
            else:
                return response.json({}, 401)

        return decorated_function

    return decorator(wrapped)


def make_response(err_code: Union[Enum, int], err_msg: str = None, data: dict = None):
    if err_msg is None:
        err_msg = str(err_code)

    return {
        "status": err_code if isinstance(err_code, int) else err_code.value,
        "msg": err_msg,
        "data": data or "{}",
    }
