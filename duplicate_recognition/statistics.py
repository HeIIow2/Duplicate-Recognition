import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class Statistics:
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
                self.logger.debug(f"{method.__name__} took {duration} seconds:")

                raise KeyboardInterrupt

            te = time.time()

            duration = te - ts
            self.timings[method.__name__].append(duration)
            self.logger.debug(f"{method.__name__} took {duration} seconds:")
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

    def __del__(self):
        self.print_stats()


STATISTICS = Statistics()


def clear_stats():
    global STATISTICS
    STATISTICS.__dict__ = Statistics().__dict__
