class BaseKPIRunner:
    def __init__(self, asset, kpi):
        self.asset = asset
        self.kpi = kpi

    def run(self):
        raise NotImplementedError("KPI runner must implement run()")
