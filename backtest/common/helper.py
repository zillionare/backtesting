from enum import Enum
from functools import wraps
from typing import Any, Union

from expiringdict import ExpiringDict
from sanic import Sanic, response

seen_requests = ExpiringDict(max_len=1000, max_age_seconds=10 * 60)


def get_account_info(token: str):
    app = Sanic.get_app("backtest")

    for item in app.ctx.cfg.accounts:
        if item["token"] == token:
            return item["name"], item["cash"], item["commission"]


def get_app_context():
    app = Sanic.get_app("backtest")
    return app.ctx


def check_token(request):
    if not request.token:
        return False

    app = Sanic.get_app("backtest")

    if request.token not in app.ctx.cfg.accounts:
        pass
        # todo
    if request.broker is None:
        account, cash, commission = get_account_info(request.token)
        from backtest.trade.broker import Broker

        request.broker = Broker(account, cash, commission)

    return True


def check_duplicated_request(request):
    request_id = request.headers.get("Request-ID")
    if request_id in seen_requests:
        return False

    seen_requests[request_id] = True
    request.json["request_id"] = request_id
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


def make_response(err_code: Union[Enum, int], data: Any = None, err_msg: str = None):
    if err_msg is None:
        err_msg = str(err_code)

    return {
        "status": err_code.value if isinstance(err_code, Enum) else err_code,
        "msg": err_msg,
        "data": data,
    }
