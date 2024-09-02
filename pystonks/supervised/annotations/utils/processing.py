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
        zvalues = [v for t, v in zip(times, values) if cmn <= t <= cmx]
        vmin = min(zvalues)
        vmax = max(zvalues)
    else:
        vmin = min(values)
        vmax = max(values)

    cavg = sum(closes) / len(closes)
    vdiff = (vmax - vmin) / 2
    return [v - vdiff + cavg for v in values]
