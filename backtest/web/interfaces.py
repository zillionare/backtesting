import logging

import arrow
from sanic import response
from sanic.blueprints import Blueprint

from backtest.common.errors import GenericErrCode
from backtest.common.helper import make_response, protected

bp = Blueprint("backtest")
logger = logging.getLogger(__name__)


@bp.route("status", methods=["GET", "POST"])
async def status(request):
    return response.json({"status": "ok", "listen": request.url})


@bp.route("buy", methods=["POST"])
@protected
async def buy(request):
    params = request.json

    security = params["security"]
    price = params["price"]
    volume = params["volume"]
    order_time = arrow.get(params["order_time"]).naive

    result = await request.ctx.broker.buy(security, price, volume, order_time)
    return response.json(result)


@bp.route("sell", methods=["POST"])
@protected
async def sell(request):
    params = request.json

    security = params["security"]
    price = params["price"]
    volume = params["volume"]
    order_time = arrow.get(params["order_time"]).naive

    result = await request.ctx.broker.sell(security, price, volume, order_time)
    return response.json(make_response(GenericErrCode.OK, data=result))


@bp.route("position", methods=["POST", "GET"])
@protected
async def position(request):
    params = request.json

    if params is None or params.get("date") is None:
        position = request.ctx.broker.position
    else:
        date = arrow.get(params.get("date")).date()
        position = request.ctx.broker.get_position(date)

    result = [{k: row[k] for k in position.dtype.names} for row in position]

    return response.json(make_response(GenericErrCode.OK, data=result))


@bp.route("info", methods=["POST", "GET"])
@protected
async def info(request):
    result = request.ctx.broker.info

    return response.json(make_response(GenericErrCode.OK, data=result))


@bp.route("returns", methods=["POST", "GET"])
@protected
async def get_returns(request):
    params = request.json

    if params is None or params.get("date") is None:
        date = None
    else:
        date = arrow.get(params.get("date")).date()

    result = request.ctx.broker.get_returns(date)

    return response.json(make_response(GenericErrCode.OK, data=result))


@bp.route("available_money", methods=["POST", "GET"])
@protected
async def available_money(request):
    cash = request.ctx.broker.cash

    return response.json(make_response(GenericErrCode.OK, data=cash))


@bp.route("available_shares", methods=["GET"])
@protected
async def available_shares(request):
    code = request.args.get("code")

    broker = request.ctx.broker
    shares = {item["code"]: item["sellable"] for item in broker.position}

    if code is None:
        return response.json(make_response(GenericErrCode.OK, data=shares))
    else:
        return response.json(make_response(GenericErrCode.OK, data=shares.get(code, 0)))


@bp.route("balance", methods=["POST", "GET"])
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
                "cash": cash,
                "market_value": market_value,
                "total": total,
                "ppnl": ppnl,
            },
        )
    )


async def metrics(request):
    params = request.json

    start = arrow.get(params["start"]).date()
    end = arrow.get(params["end"]).date()

    metrics = request.ctx.broker.metrics(start, end)
    return response.json(make_response(GenericErrCode.OK, data=metrics))
