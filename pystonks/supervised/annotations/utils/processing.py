from typing import List

from pystonks.supervised.annotations.utils.models import PlotStateInfo, GeneralStockPlotInfo


def place_on_avg(
        state: PlotStateInfo, data: GeneralStockPlotInfo,
        times: List[float], values: List[float]
) -> List[float]:
    closes = data.closes
    if state.is_zoomed:
        cmn, cmx = state.zoom_lim
        closes = [c for t, c in zip(data.times, data.closes) if cmn <= t <= cmx]

    cavg = sum(closes) / len(closes)
    vdiff = sum(values) / len(values)   # (max(values) - min(values)) / 2
    return [v - vdiff + cavg for v in values]
