import logging

import arrow
from omicron import math_round
from sanic import response
from sanic.blueprints import Blueprint

from backtest.common.errors import AccountError, GenericErrCode
from backtest.common.helper import jsonify, make_response, protected, protected_admin
from backtest.trade.broker import Broker
from backtest.trade.types import position_dtype

bp = Blueprint("backtest")
logger = logging.getLogger(__name__)


@bp.route("status", methods=["GET"])
async def status(request):
    return response.json({"status": "ok", "listen": request.url})


@bp.route("accounts", methods=["POST"])
@protected_admin
async def create_account(request):
    """创建一个模拟盘账户"""
    params = request.json or {}

    name = params["name"]
    token = params["token"]
    commission = params["commission"]
    capital = params["capital"]

    if any([name is None, capital is None, token is None, commission is None]):
        msg = f"必须传入name: {name}, capital: {capital}, token: {token}, commission: {commission}"

        return response.text(f"Bad parameter: {msg}", status=400)

    accounts = request.app.ctx.accounts

    try:
        result = accounts.create_account(name, token, capital, commission)
        start = result["account_start_date"]
        if start is not None:
            result["account_start_date"] = arrow.get(start).format("YYYY-MM-DD")
    except AccountError as e:
        return response.text(e.message, status=400)

    return response.json(make_response(GenericErrCode.OK, data=result))


@bp.route("start_backtest", methods=["POST"])
async def start_backtest(request):
    params = request.json or {}

    try:
        name = params["name"]
        token = params["token"]
        start = arrow.get(params["start"]).date()
        end = arrow.get(params["end"]).date()
        capital = params["capital"]
        commission = params["commission"]
    except Exception:
        return response.text("Bad parameter: name, version, start, end", status=400)

    accounts = request.app.ctx.accounts
    try:
        result = accounts.create_account(
            name, token, capital, commission, start=start, end=end
        )
        return response.json(make_response(GenericErrCode.OK, data=jsonify(result)))
    except AccountError as e:
        return response.text(e.message, status=400)


@bp.route("accounts", methods=["GET"])
@protected_admin
async def list_accounts(request):
    mode = request.args.get("mode", "all")

    accounts = request.app.ctx.accounts

    result = accounts.list_accounts(mode)
    for account in result:
        date = account["account_start_date"]
        if date is not None:
            account["account_start_date"] = arrow.get(date).format("YYYY-MM-DD")

    return response.json(make_response(GenericErrCode.OK, data=result))


@bp.route("buy", methods=["POST"])
@protected
async def buy(request):
    """买入

    Args:
        request : 参数以json方式传入， 包含：
            - security : 证券代码
            - price: 买入价格,如果为None，则意味着以市价买入
            - volume: 买入数量
            - order_time: 下单时间
    """
    params = request.json or {}

    security = params["security"]
    price = params["price"]
    volume = params["volume"]
    order_time = arrow.get(params["order_time"]).naive

    try:
        result = await request.ctx.broker.buy(security, price, volume, order_time)
        return response.json(jsonify(result))
    except Exception as e:
        logger.exception(e)
        return response.text(str(e), status=500)


@bp.route("market_buy", methods=["POST"])
@protected
async def market_buy(request):
    """市价买入

    Args:
        request : 参数以json方式传入， 包含：
            security : 证券代码
            volume: 买入数量
            order_time: 下单时间
    """
    params = request.json or {}

    security = params["security"]
    volume = params["volume"]
    order_time = arrow.get(params["order_time"]).naive

    try:
        result = await request.ctx.broker.buy(security, None, volume, order_time)
        return response.json(jsonify(result))
    except Exception as e:
        logger.exception(e)
        return response.text(str(e), status=500)


@bp.route("sell", methods=["POST"])
@protected
async def sell(request):
    params = request.json or {}

    security = params["security"]
    price = params["price"]
    volume = params["volume"]
    order_time = arrow.get(params["order_time"]).naive

    try:
        result = await request.ctx.broker.sell(security, price, volume, order_time)
        return response.json(jsonify(result))
    except Exception as e:
        logger.exception(e)
        return response.text(str(e), status=500)


@bp.route("sell_percent", methods=["POST"])
@protected
async def sell_percent(request):
    params = request.json or {}

    security = params["security"]
    price = params["price"]
    percent = params["percent"]
    order_time = arrow.get(params["order_time"]).naive

    try:
        assert 0 < percent <= 1.0, "percent must be between 0 and 1.0"
        broker: Broker = request.ctx.broker
        position = broker.get_position(order_time.date())
        sellable = position[position["security"] == security][0]["sellable"]

        volume = math_round(sellable * percent / 100, 0) * 100

        result = await request.ctx.broker.sell(security, price, volume, order_time)
        return response.json(jsonify(result))
    except Exception as e:
        logger.exception(e)
        return response.text(str(e), status=500)


@bp.route("market_sell", methods=["POST"])
@protected
async def market_sell(request):
    params = request.json or {}

    security = params["security"]
    volume = params["volume"]
    order_time = arrow.get(params["order_time"]).naive

    try:
        result = await request.ctx.broker.sell(security, None, volume, order_time)
        return response.json(jsonify(result))
    except Exception as e:
        logger.exception(e)
        return response.text(str(e), status=500)


@bp.route("positions", methods=["GET"])
@protected
async def positions(request):
    date = request.args.get("date")

    if date is None:
        position = request.ctx.broker.position
    else:
        date = arrow.get(date).date()
        position = request.ctx.broker.get_position(date)

    result = [{k: row[k] for k in position.dtype.names} for row in position]

    return response.json(make_response(GenericErrCode.OK, data=jsonify(result)))


@bp.route("info", methods=["GET"])
@protected
async def info(request):
    result = await request.ctx.broker.info()

    return response.json(make_response(GenericErrCode.OK, data=jsonify(result)))


@bp.route("returns", methods=["GET"])
@protected
async def get_returns(request):
    date = request.args.get("date")
    result = await request.ctx.broker.get_returns(date)

    return response.json(make_response(GenericErrCode.OK, data=jsonify(result)))


@bp.route("available_money", methods=["GET"])
@protected
async def available_money(request):
    cash = request.ctx.broker.cash

    return response.json(make_response(GenericErrCode.OK, data=cash))


@bp.route("available_shares", methods=["GET"])
@protected
async def available_shares(request):
    code = request.args.get("code")

    broker = request.ctx.broker
    shares = {item["security"]: item["sellable"] for item in broker.position}

    if code is None:
        return response.json(make_response(GenericErrCode.OK, data=shares))
    else:
        return response.json(make_response(GenericErrCode.OK, data=shares.get(code, 0)))


@bp.route("balance", methods=["GET"])
@protected
async def balance(request):
    broker = request.ctx.broker

    account = broker.account_name
    pnl = broker.assets - broker.capital
    cash = broker.cash
    market_value = broker.assets - cash
    total = broker.assets
    ppnl = pnl / broker.capital

    return response.json(
        make_response(
            GenericErrCode.OK,
            data={
                "account": account,
                "pnl": pnl,
                "available": cash,
                "market_value": market_value,
                "total": total,
                "ppnl": ppnl,
            },
        )
    )


@bp.route("metrics", methods=["GET"])
@protected
async def metrics(request):
    start = request.args.get("start")
    end = request.args.get("end")
    baseline = request.args.get("baseline")

    if start:
        start = arrow.get(start).date()

    if end:
        end = arrow.get(end).date()

    metrics = await request.ctx.broker.metrics(start, end, baseline)

    return response.json(make_response(GenericErrCode.OK, data=jsonify(metrics)))


@bp.route("bills", methods=["GET"])
@protected
async def bills(request):
    results = {}

    broker: Broker = request.ctx.broker

    results["tx"] = broker.transactions
    results["trades"] = broker.trades
    results["positions"] = broker._positions[list(position_dtype.names)].astype(
        position_dtype
    )

    await broker.recalc_assets()
    results["assets"] = broker._assets

    return response.json(make_response(GenericErrCode.OK, data=jsonify(results)))


@bp.route("accounts", methods=["DELETE"])
@protected_admin
async def delete_accounts(request):
    account_to_delete = request.args.get("name", None)
    accounts = request.app.ctx.accounts

    n_accounts = accounts.delete_accounts(account_to_delete)
    return response.json(make_response(GenericErrCode.OK, data=n_accounts))
