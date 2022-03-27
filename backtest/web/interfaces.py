import logging

import arrow
from sanic import response
from sanic.blueprints import Blueprint

from backtest.common.errors import AccountConflictError, GenericErrCode
from backtest.common.helper import make_response, protected

bp = Blueprint("backtest")
logger = logging.getLogger(__name__)


@bp.route("status", methods=["GET", "POST"])
async def status(request):
    return response.json({"status": "ok", "listen": request.url})


@bp.route("accounts", methods=["POST"])
async def create_account(request):
    params = request.json or {}

    name = params["name"]
    capital = params["capital"]
    token = params["token"]
    commission = params["commission"]

    if any([name is None, capital is None, token is None, commission is None]):
        msg = f"必须传入name: {name}, capital: {capital}, token: {token}, commission: {commission}"

        return response.text(f"Bad parameter: {msg}", status=400)

    accounts = request.app.ctx.accounts

    try:
        result = accounts.create_account(token, name, capital, commission)
        start = result["account_start_date"]
        if start is not None:
            result["account_start_date"] = arrow.get(start).format("YYYY-MM-DD")
    except AccountConflictError as e:
        return response.text(e.message, status=400)

    return response.json(make_response(GenericErrCode.OK, data=result))


@bp.route("accounts", methods=["GET"])
async def list_accounts(request):
    accounts = request.app.ctx.accounts

    result = accounts.list_accounts()
    for account in result:
        date = account["account_start_date"]
        if date is not None:
            account["account_start_date"] = arrow.get(date).format("YYYY-MM-DD")

    return response.json(make_response(GenericErrCode.OK, data=result))


@bp.route("buy", methods=["POST"])
@protected
async def buy(request):
    params = request.json or {}

    security = params["security"]
    price = params["price"]
    volume = params["volume"]
    order_time = arrow.get(params["order_time"]).naive

    result = await request.ctx.broker.buy(security, price, volume, order_time)
    return response.json(result)


@bp.route("sell", methods=["POST"])
@protected
async def sell(request):
    params = request.json or {}

    security = params["security"]
    price = params["price"]
    volume = params["volume"]
    order_time = arrow.get(params["order_time"]).naive

    result = await request.ctx.broker.sell(security, price, volume, order_time)
    return response.json(result)


@bp.route("positions", methods=["POST", "GET"])
@protected
async def positions(request):
    params = request.json or {}

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

    last_trade = result["last_trade"]
    if last_trade is not None:
        result["last_trade"] = arrow.get(last_trade).format("YYYY-MM-DD")

    start = result["start"]
    if start is not None:
        result["start"] = arrow.get(start).format("YYYY-MM-DD")

    return response.json(make_response(GenericErrCode.OK, data=result))


@bp.route("returns", methods=["POST", "GET"])
@protected
async def get_returns(request):
    params = request.json or {}

    if params is None or params.get("date") is None:
        date = None
    else:
        date = arrow.get(params.get("date")).date()

    result = request.ctx.broker.get_returns(date).tolist()

    return response.json(make_response(GenericErrCode.OK, data=result))


@bp.route("available_money", methods=["POST", "GET"])
@protected
async def available_money(request):
    cash = request.ctx.broker.cash

    return response.json(make_response(GenericErrCode.OK, data=cash))


@bp.route("available_shares", methods=["GET", "POST"])
@protected
async def available_shares(request):
    code = request.args.get("code")

    broker = request.ctx.broker
    shares = {item["security"]: item["sellable"] for item in broker.position}

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
                "available": cash,
                "market_value": market_value,
                "total": total,
                "ppnl": ppnl,
            },
        )
    )


@bp.route("metrics", methods=["POST", "GET"])
@protected
async def metrics(request):
    params = request.json or {}

    start = params.get("start")
    end = params.get("end")

    if start:
        start = arrow.get(start).date()

    if end:
        end = arrow.get(end).date()

    metrics = request.ctx.broker.metrics(start, end)
    metrics["start"] = arrow.get(metrics["start"]).format("YYYY-MM-DD")
    metrics["end"] = arrow.get(metrics["end"]).format("YYYY-MM-DD")

    return response.json(make_response(GenericErrCode.OK, data=metrics))
