import logging
import pickle

import arrow
import pkg_resources
from omicron import math_round
from sanic import response
from sanic.blueprints import Blueprint

from backtest.common.errors import AccountError
from backtest.common.helper import jsonify, protected, protected_admin
from backtest.trade.broker import Broker
from backtest.trade.datatypes import position_dtype

ver = pkg_resources.get_distribution("zillionare-backtest").parsed_version

bp = Blueprint("backtest")
logger = logging.getLogger(__name__)


@bp.route("status", methods=["GET"])
async def status(request):
    return response.json(
        {"status": "ok", "listen": request.url, "version": ver.base_version}
    )


@bp.route("start_backtest", methods=["POST"])
async def start_backtest(request):
    """启动回测

    启动回测时，将为接下来的回测创建一个新的账户。

    Args:
        request Request: 包含以下字段的请求对象

            - name, 账户名称
            - token,账户token
            - principal,账户初始资金
            - commission,账户手续费率
            - start,回测开始日期，格式为YYYY-MM-DD
            - end,回测结束日期，格式为YYYY-MM-DD

    Returns:

        json: 包含以下字段的json对象

        - account_name, str
        - token, str
        - account_start_date, str
        - principal, float

    """
    params = request.json or {}

    try:
        name = params["name"]
        token = params["token"]
        start = arrow.get(params["start"]).date()
        end = arrow.get(params["end"]).date()
        principal = params["principal"]
        commission = params["commission"]
    except KeyError as e:
        logger.warning(f"parameter {e} is required")
        return response.text(f"parameter {e} is required", status=499)
    except Exception as e:
        logger.exception(e)
        return response.text(
            "parameter error: name, token, start, end, principal, commission",
            status=499,
        )

    accounts = request.app.ctx.accounts
    try:
        result = accounts.create_account(
            name, token, principal, commission, start=start, end=end
        )
        logger.info("backtest account created:", result)
        return response.json(jsonify(result))
    except AccountError as e:
        return response.text(e.message, status=499)


@bp.route("accounts", methods=["GET"])
@protected_admin
async def list_accounts(request):
    mode = request.args.get("mode", "all")

    accounts = request.app.ctx.accounts
    result = accounts.list_accounts(mode)

    return response.json(jsonify(result))


@bp.route("buy", methods=["POST"])
@protected
async def buy(request):
    """买入

    Args:
        request Request: 参数以json方式传入， 包含：

            - security : 证券代码
            - price: 买入价格,如果为None，则意味着以市价买入
            - volume: 买入数量
            - order_time: 下单时间

    Returns:
        Response: 买入结果, 字典，包含以下字段：

        - tid: str, 交易id
        - eid: str, 委托id
        - security: str, 证券代码
        - order_side: str, 买入/卖出
        - price: float, 成交均价
        - filled: float, 成交数量
        - time: str, 下单时间
        - trade_fees: float, 手续费

    """
    params = request.json or {}

    security = params["security"]
    price = params["price"]
    volume = params["volume"]
    order_time = arrow.get(params["order_time"]).naive

    result = await request.ctx.broker.buy(security, price, volume, order_time)
    return response.json(jsonify(result))


@bp.route("market_buy", methods=["POST"])
@protected
async def market_buy(request):
    """市价买入

    Args:
        request Request: 参数以json方式传入， 包含

            - security: 证券代码
            - volume: 买入数量
            - order_time: 下单时间
    Returns:
        Response: 买入结果, 请参考[backtest.web.interfaces.buy][]

    """
    params = request.json or {}

    security = params["security"]
    volume = params["volume"]
    order_time = arrow.get(params["order_time"]).naive

    result = await request.ctx.broker.buy(security, None, volume, order_time)
    return response.json(jsonify(result))


@bp.route("sell", methods=["POST"])
@protected
async def sell(request):
    """卖出证券

    Args:
        request: 参数以json方式传入， 包含：

            - security : 证券代码
            - price: 卖出价格,如果为None，则意味着以市价卖出
            - volume: 卖出数量
            - order_time: 下单时间

    Returns:
        Response: 参考[backtest.web.interfaces.buy][]
    """
    params = request.json or {}

    security = params["security"]
    price = params["price"]
    volume = params["volume"]
    order_time = arrow.get(params["order_time"]).naive

    result = await request.ctx.broker.sell(security, price, volume, order_time)
    return response.json(jsonify(result))


@bp.route("sell_percent", methods=["POST"])
@protected
async def sell_percent(request):
    """卖出证券

    Args:
        request Request: 参数以json方式传入， 包含

            - security: 证券代码
            - percent: 卖出比例
            - order_time: 下单时间
            - price: 卖出价格,如果为None，则意味着以市价卖出

    Returns:
        Response: 参考[backtest.web.interfaces.buy][]
    """
    params = request.json or {}

    security = params["security"]
    price = params["price"]
    percent = params["percent"]
    order_time = arrow.get(params["order_time"]).naive

    assert 0 < percent <= 1.0, "percent must be between 0 and 1.0"
    broker: Broker = request.ctx.broker
    position = broker.get_position(order_time.date())
    sellable = position[position["security"] == security][0]["sellable"]

    volume = math_round(sellable * percent / 100, 0) * 100

    result = await request.ctx.broker.sell(security, price, volume, order_time)
    return response.json(jsonify(result))


@bp.route("market_sell", methods=["POST"])
@protected
async def market_sell(request):
    """以市价卖出证券

    Args:
        request : 以json方式传入，包含以下字段

            - security : 证券代码
            - volume: 卖出数量
            - order_time: 下单时间

    Returns:
        Response: 参考[backtest.web.interfaces.buy][]
    """
    params = request.json or {}

    security = params["security"]
    volume = params["volume"]
    order_time = arrow.get(params["order_time"]).naive

    result = await request.ctx.broker.sell(security, None, volume, order_time)
    return response.json(jsonify(result))


@bp.route("positions", methods=["GET"])
@protected
async def positions(request):
    """获取持仓信息

    Args:
        request Request:以args方式传入，包含以下字段:

            - date: 日期，格式为YYYY-MM-DD,待获取持仓信息的日期

    Returns:
        Response: 结果以binary方式返回。结果为一个numpy structured array数组，其dtype为[backtest.trade.datatypes.position_dtype][]

    """
    date = request.args.get("date")

    if date is None:
        position = request.ctx.broker.position
    else:
        date = arrow.get(date).date()
        position = request.ctx.broker.get_position(date)

    return response.raw(pickle.dumps(position))


@bp.route("info", methods=["GET"])
@protected
async def info(request):
    """获取账户信息

    Args:
        request Request: 以args方式传入，包含以下字段

            - date: 日期，格式为YYYY-MM-DD,待获取账户信息的日期，如果为空，则意味着取当前日期的账户信息

    Returns:

        Response: 结果以binary方式返回。结果为一个dict，其中包含以下字段：

        - name: str, 账户名
        - principal: float, 初始资金
        - assets: float, 当前资产
        - start: datetime.date, 账户创建时间
        - last_trade: datetime.date, 最后一笔交易日期
        - available: float, 可用资金
        - market_value: 股票市值
        - pnl: 盈亏(绝对值)
        - ppnl: 盈亏(百分比)，即pnl/principal
        - positions: 当前持仓，dtype为[backtest.trade.datatypes.position_dtype][]的numpy structured array

    """
    date = request.args.get("date")
    result = await request.ctx.broker.info(date)
    return response.raw(pickle.dumps(result))


@bp.route("metrics", methods=["GET"])
@protected
async def metrics(request):
    """获取回测的评估指标信息

    Args:
        request : 以args方式传入，包含以下字段

            - start: 开始时间，格式为YYYY-MM-DD
            - end: 结束时间，格式为YYYY-MM-DD
            - baseline: str, 用来做对比的证券代码，默认为空，即不做对比

    Returns:

        Response: 结果以binary方式返回,参考[backtest.trade.broker.Broker.metrics][]

    """
    start = request.args.get("start")
    end = request.args.get("end")
    baseline = request.args.get("baseline")

    if start:
        start = arrow.get(start).date()

    if end:
        end = arrow.get(end).date()

    metrics = await request.ctx.broker.metrics(start, end, baseline)
    return response.raw(pickle.dumps(metrics))


@bp.route("bills", methods=["GET"])
@protected
async def bills(request):
    """获取交易记录

    Returns:
        Response: 以binary方式返回。结果为一个字典，包括以下字段：

        - tx: 配对的交易记录
        - trades: 成交记录
        - positions: 持仓记录
        - assets: 每日市值

    """
    results = {}

    broker: Broker = request.ctx.broker

    results["tx"] = broker.transactions
    results["trades"] = broker.trades
    results["positions"] = broker._positions[list(position_dtype.names)].astype(
        position_dtype
    )

    await broker.recalc_assets()
    results["assets"] = broker._assets
    return response.json(jsonify(results))


@bp.route("accounts", methods=["DELETE"])
@protected
async def delete_accounts(request):
    """删除账户

    当提供了账户名`name`和token（通过headers传递)时，如果name与token能够匹配，则删除`name`账户。
    Args:
        request Request: 通过params传递以下字段

            - name, 待删除的账户名。如果为空，且提供了admin token，则删除全部账户。

    """
    account_to_delete = request.args.get("name", None)
    accounts = request.app.ctx.accounts

    if account_to_delete is None:
        if request.ctx.broker.account_name == "admin":
            accounts.delete_accounts()
        else:
            return response.text("admin account required", status=403)

    if account_to_delete == request.ctx.broker.account_name:
        accounts.delete_accounts(account_to_delete)
