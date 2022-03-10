from coretypes import Frame, FrameType, SecurityType, bars_dtype

from backtest.data.basefeed import BaseFeed


class FileFeed(BaseFeed):
    def __init__(self, **kwargs):
        pass

    async def init(self):
        pass

    async def get_bars(
        self, security: str, ft: FrameType, start: Frame, end: Frame, n: int, fq=True
    ) -> list:
        pass
