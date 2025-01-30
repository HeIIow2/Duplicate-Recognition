import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List


@dataclass
class Statistics:
    name: str = "other"

    timings: Dict[str, List[float]] = field(default_factory=lambda: defaultdict(list))
    compared_field_total: int = 0
    compared_entity_total: int = 0
    compared_pairs_total: int = 0

    logger = logging.getLogger("statistics")

    def compare_wrapper(self, method):
        def _inner(*args, **kw):
            self.compared_pairs_total += 1
            return method(*args, **kw)

        return _inner

    def timeit(self, method):
        logger = logging.getLogger("timeit")

        def timed(*args, **kw):
            ts = time.time()
            try:
                result = method(*args, **kw)
            except KeyboardInterrupt:
                te = time.time()
                duration = te - ts
                self.timings[method.__name__].append(duration)
                self.logger.debug("%s took %d seconds:", method.__name__, duration)

                raise KeyboardInterrupt

            te = time.time()

            duration = te - ts
            self.timings[method.__name__].append(duration)
            self.logger.debug("%s took %d seconds:", method.__name__, duration)
            return result

        return timed

    def silent_timeit(self, method):
        def timed(*args, **kw):
            ts = time.time()
            result = method(*args, **kw)
            te = time.time()

            self.timings[method.__name__].append(te - ts)
            return result

        return timed

    def promise_timeit(self, method):
        def timed(*args, **kw):
            ts = time.time()

            def _callback():
                te = time.time()
                self.timings[method.__name__].append(te - ts)

            kw["callback"] = _callback

            return method(*args, **kw)

        return timed

    def print_stats(self):
        # this tries to ask if there are any timings
        if len(self.timings) <= 0:
            return

        timing_str = "Timings:\n"

        for name, timing in self.timings.items():
            if len(timing) == 1:
                timing_str += f"{name}: {round(timing[0], 5)} seconds\n"
            else:
                timing_str += f"{name}\n"
                timing_str += f"\taverage: {sum(timing) / len(timing)}\n"
                timing_str += f"\tnumbers called: {len(timing)}\n"
                timing_str += f"\ttotal duration: {sum(timing)}\n"

        self.logger.info(timing_str)

        comp_str = "Comparisons:\n"
        if self.compared_entity_total > 0:
            comp_str += f"Average compared fields per entity: {self.compared_field_total / self.compared_entity_total}\n"
        comp_str += f"Comparison pairs: {self.compared_pairs_total}"

        self.logger.info(comp_str)

    @property
    def df_dict(self) -> dict:
        return {
            "name": self.name,
            "compared_field_total": self.compared_field_total,
            "compared_entity_total": self.compared_entity_total,
            "compared_pairs_total": self.compared_pairs_total,
            "compared_field_avg": self.compared_field_total / self.compared_entity_total if self.compared_entity_total > 0 else 0,
        }


class DASHBOARD:
    logger = logging.getLogger("dashboard")
    statistics_history: List[Statistics] = []

    STATISTICS = Statistics()

    @classmethod
    def add_statistics(cls, name: str = "other", **kwargs):
        if len(cls.statistics_history) > 0:
            last_history = cls.statistics_history[-1]
            last_history.print_stats()

            if last_history.name == name:
                cls.logger.info("Statistics with name '%s' already exists.", name)
                return
        
        

        s = Statistics(name=name, **kwargs)
        cls.statistics_history.append(s)
        cls.STATISTICS.__dict__ = s.__dict__

    @classmethod
    @property
    def file_name(cls) -> str:
        return f"statistics_{datetime.now().date().isoformat()}"

    @classmethod
    @property
    def dataframe_generator(cls) -> List[dict]:
        """
        ```python
        import pandas as pd

        df = pd.DataFrame(DASHBOARD.dataframe_generator)
        ```
        """
        
        return [stat.df_dict for stat in cls.statistics_history]
