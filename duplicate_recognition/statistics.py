import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List
import math


@dataclass
class Statistics:
    timings: Dict[str, List[float]] = field(default_factory=lambda: defaultdict(list))
    compared_field_total: int = 0
    compared_entity_total: int = 0
    compared_pairs_total: int = 0

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
                logger.info(f"{method.__name__} took {duration} seconds:")

                raise KeyboardInterrupt

            te = time.time()

            duration = te - ts
            self.timings[method.__name__].append(duration)
            logger.info(f"{method.__name__} took {duration} seconds:")
            return result

        return timed

    def __del__(self):
        print("Timings:")
        for name, timing in self.timings.items():
            if len(timing) == 1:
                print(f"{name}: {timing[0]} seconds")
            else:
                print(f"{name}: on avr. {sum(timing) / len(timing)} seconds")
                print(*timing, sep=", ")

        print()
        print("Comparisons:")
        if self.compared_pairs_total > 0:
            print(f"Average compared entity count per comparison: "
                  f"{self.compared_entity_total / self.compared_pairs_total}")
        print(f"Comparison pairs: {self.compared_pairs_total}")


STATISTICS = Statistics()
