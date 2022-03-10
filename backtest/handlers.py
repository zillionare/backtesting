import logging

from sanic import response
from sanic.blueprints import Blueprint

from backtest.helper import broker, protected

bp = Blueprint("backtest", url_prefix="/backtest/")
logger = logging.getLogger(__name__)


@bp.route("/api/trade/v0.1/buy", methods=["POST"])
@protected
async def buy(request):
    params = request.json

    security = params["security"]
    price = params["price"]
    volume = params["volume"]
    timeout = params["timeout"]
    order_time = params["order_time"]

    result = await broker.buy(security, price, volume, order_time, timeout)
    return response.json(result)


@bp.route("/api/trade/v0.1/sell", methods=["POST"])
@protected
async def sell(request):
    params = request.json

    security = params["security"]
    price = params["price"]
    volume = params["volume"]
    timeout = params["timeout"]
    order_time = params["order_time"]

    result = await broker.sell(security, price, volume, order_time, timeout)
    return response.json(result)
