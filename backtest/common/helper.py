import traceback
from functools import wraps

import cfg4py
import numpy as np
from coretypes.errors.trade import TradeError
from expiringdict import ExpiringDict
from omicron.core.backtestlog import BacktestLogger
from sanic import Sanic, response
from tabulate import tabulate

seen_requests = ExpiringDict(max_len=1000, max_age_seconds=10 * 60)

logger = BacktestLogger.getLogger(__name__)


def get_app_context():
    app = Sanic.get_app("backtest")
    return app.ctx


def check_token(request):
    if not request.token:
        return False

    app = Sanic.get_app("backtest")

    if check_admin_token(request):
        return True

    if app.ctx.accounts.is_valid(request.token):
        request.ctx.broker = app.ctx.accounts.get_broker(request.token)
        return True
    else:
        return False


def check_admin_token(request):
    if not request.token:
        return False

    app = Sanic.get_app("backtest")

    if app.ctx.accounts.is_admin(request.token):
        cfg = cfg4py.get_instance()
        request.ctx.broker = app.ctx.accounts.get_broker(cfg.auth.admin)
        return True
    else:
        return False


def check_duplicated_request(request):
    request_id = request.headers.get("Request-ID")
    if request_id in seen_requests:
        logger.info("duplicated request: [%s]", request_id)
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
            params = request.json or request.args
            command = request.server_path.split("/")[-1]

            if is_authenticated and not is_duplicated:
                try:
                    logger.info("received request: %s, params %s", command, params)
                    result = await f(request, *args, **kwargs)
                    logger.info("finished request: %s, params %s", command, params)
                    return result
                except Exception as e:
                    logger.exception(e)
                    logger.warning("%s error: %s", f.__name__, params)
                    if isinstance(e, TradeError):
                        return response.json(e.as_json(), status=499)
                    else:
                        e2 = TradeError(str(e))
                        return response.json(e2.as_json(), status=499)
            elif not is_authenticated:
                logger.warning("token is invalid: [%s]", request.token)
                return response.json({"msg": "token is invalid"}, 401)
            elif is_duplicated:
                return response.json({"msg": "duplicated request"}, 200)

        return decorated_function

    return decorator(wrapped)


def protected_admin(wrapped):
    """check token and duplicated request"""

    def decorator(f):
        @wraps(f)
        async def decorated_function(request, *args, **kwargs):
            is_authenticated = check_admin_token(request)
            is_duplicated = check_duplicated_request(request)

            if is_authenticated and not is_duplicated:
                try:
                    result = await f(request, *args, **kwargs)
                    return result
                except Exception as e:
                    logger.exception(e)
                    return response.text(str(e), status=500)
            elif not is_authenticated:
                logger.warning("admin token is invalid: [%s]", request.token)
                return response.text(f"token({request.token}) is invalid", 401)
            elif is_duplicated:
                return response.text(
                    f"duplicated request: {request.ctx.request_id}", 200
                )

        return decorated_function

    return decorator(wrapped)


def jsonify(obj) -> dict:
    """将对象`obj`转换成为可以通过json.dumps序列化的字典

    本方法可以将str, int, float, bool, datetime.date, datetime.datetime, 或者提供了isoformat方法的其它时间类型， 提供了to_dict方法的对象类型（比如自定义对象），提供了tolist或者__iter__方法的序列对象（比如numpy数组），或者提供了__dict__方法的对象，以及上述对象的复合对象，都可以被正确地转换。

    转换中依照以下顺序进行：

    1. 简单类型，如str, int, float, bool
    2. 提供了to_dict的自定义类型
    3. 如果是numpy数组，优先按tolist方法进行转换
    4. 如果是提供了isoformat的时间类型，优先转换
    5. 如果对象是dict, 按dict进行转换
    6. 如果对象提供了__iter__方法，按序列进行转换
    7. 如果对象提供了__dict__方法，按dict进行转换
    8. 抛出异常
    Args:
        obj : object to convert

    Returns:
        A dict able to be json dumps
    """
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj  # type: ignore
    elif getattr(obj, "to_dict", False):
        return jsonify(obj.to_dict())
    elif getattr(obj, "tolist", False):  # for numpy array
        return jsonify(obj.tolist())
    elif getattr(obj, "isoformat", False):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: jsonify(v) for k, v in obj.items()}
    elif getattr(obj, "__iter__", False):  # 注意dict类型也有__iter__
        return [jsonify(x) for x in obj]  # type: ignore
    elif getattr(obj, "__dict__", False):
        return {k: jsonify(v) for k, v in obj.__dict__.items()}
    else:
        raise ValueError(f"{obj} is not jsonable")


def tabulate_numpy_array(arr: np.ndarray) -> str:
    """将numpy structured array 格式化为表格对齐的字符串

    Args:
        arr : _description_

    Returns:
        _description_
    """
    table = tabulate(arr, headers=arr.dtype.names, tablefmt="fancy_grid")
    return table
