import logging

from sanic import Sanic, response
from sanic.blueprints import Blueprint

app = Sanic.get_app("backtest")
broker = app.ctx.broker
bp = Blueprint("backtest", url_prefix="/backtest/")
logger = logging.getLogger(__name__)


@bp.route("buy", methods=["POST"])
async def buy(request):
    params = request.json

    security = params["security"]
    price = params["price"]
    volume = params["volume"]
    timeout = params["timeout"]

    result = await broker.buy(security, price, volume, timeout)
    return response.json(result)


@bp.route("sell", methods=["POST"])
async def sell(request):
    params = request.json

    security = params["security"]
    price = params["price"]
    volume = params["volume"]
    timeout = params["timeout"]

    result = await broker.sell(security, price, volume, timeout)
    return response.json(result)
