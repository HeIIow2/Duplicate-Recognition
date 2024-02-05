import logging
from functools import lru_cache
from typing import Callable, Dict, Any
from pathlib import Path
import json

from Levenshtein import distance
import pycountry
import tempfile

from .utils import Algorithm
from .statistics import STATISTICS


class CountryComparisons:
    def __init__(self):
        self._fuzzy_map = {}
        self._fuzzy_map_cache = Path(tempfile.gettempdir(), "pycountry_fuzzy_map.json")
        if self._fuzzy_map_cache.is_file():
            with self._fuzzy_map_cache.open("r") as f:
                self._fuzzy_map = json.load(f)

        # Test if this pull request already has been merged https://github.com/pycountry/pycountry/pull/210
        self._fuzzy_kwargs = {"return_first": True}
        try:
            pycountry.countries.search_fuzzy("DE", **self._fuzzy_kwargs)
        except TypeError:
            logging.warning("pycountry version is too old, fuzzy search will be slower.")
            self._fuzzy_kwargs = {}

    @STATISTICS.silent_timeit
    def get_countries(self, query: str) -> str:
        if query in self._fuzzy_map:
            return self._fuzzy_map[query]

        try:
            r = pycountry.countries.search_fuzzy(query, **self._fuzzy_kwargs)
        except LookupError:
            return query
        if len(r) == 0:
            return query

        result = r[0].name

        self._fuzzy_map[query] = result
        self._fuzzy_map[result] = result

        return result

    def compare(self, a: str, b: str) -> float:
        """
        :param a: The first string to compare
        :param b: The second string to compare

        :return: A float between 0 and 1 representing similarity of a and b.
        """

        a = self.get_countries(a)
        b = self.get_countries(b)

        return a == b

    def __del__(self):
        with self._fuzzy_map_cache.open("w") as f:
            json.dump(self._fuzzy_map, f)


@lru_cache()
@STATISTICS.silent_timeit
def phonetic_distance(a: str, b: str) -> float:
    """
    :param a: The first string to compare
    :param b: The second string to compare

    :return: A float between 0 and 1 representing similarity of self and other.
    """
    max_length = max(len(a), len(b))
    if max_length == 0:
        return 0

    d = distance(a, b)
    return 1 - (d / max_length)


@lru_cache()
def compare_stripped_numbers(a: str, b: str) -> float:
    """
    :param a: The first string to compare
    :param b: The second string to compare

    :return: A float between 0 and 1 representing similarity of self and other.

    strip all non-numeric characters, and do a comparison
    """
    return ''.join(filter(str.isdigit, a)) == ''.join(filter(str.isdigit, b))


@lru_cache()
def compare_url(a: str, b: str) -> float:
    """
    :param a: The first string to compare
    :param b: The second string to compare

    :return: A float between 0 and 1 representing similarity of self and other.
    """
    a = a.replace("/", "").replace("www.", "").replace("http:", "").replace("https:", "")
    b = b.replace("/", "").replace("www.", "").replace("http:", "").replace("https:", "")
    return phonetic_distance(a, b)


def compare_email(a: str, b: str) -> float:
    if not ("@" in a and "@" in b):
        return phonetic_distance(a, b)

    a_name, a_domain = a.split("@", 1)
    b_name, b_domain = b.split("@", 1)

    if a_domain != b_domain:
        return 0

    a_name = a_name.rsplit("+", 1)[0]
    b_name = b_name.rsplit("+", 1)[0]

    return int(a_name == b_name)


# the comparisons with the countries need state, thus they have to be initialized
country_state = CountryComparisons()


FIELD_ALGORITHMS: Dict[Algorithm, Callable[[Any, Any], float]] = {
    Algorithm.EQUALITY: lambda self, other: self == other,
    Algorithm.PHONETIC_DISTANCE: phonetic_distance,
    Algorithm.URL: compare_url,
    Algorithm.COUNTRY: country_state.compare,
    Algorithm.VAT_ID: compare_stripped_numbers,
    Algorithm.PHONE: compare_stripped_numbers,
    Algorithm.EMAIL: compare_email,
}


def compare_fields(self: Any, other: Any, match_type: Algorithm) -> float:
    """
    :param self: The first value to compare
    :param other: The second value to compare
    :param match_type: The type of matching algorithm to use

    :return: A float between 0 and 1 representing similarity of self and other.
    """

    if match_type not in FIELD_ALGORITHMS:
        raise NotImplementedError(f"Unknown match_type {match_type}")

    return FIELD_ALGORITHMS[match_type](self, other)
