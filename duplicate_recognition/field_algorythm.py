from functools import lru_cache
from typing import Callable, Dict, Any

from Levenshtein import distance
import pycountry

from .utils import Algorithm


@lru_cache()
def compare_country(self: str, other: str) -> float:
    """
    :param self: The first string to compare
    :param other: The second string to compare

    :return: A float between 0 and 1 representing similarity of self and other.
    """

    @lru_cache()
    def get_countries(raw_country: str) -> str:
        try:
            r = pycountry.countries.search_fuzzy(raw_country)
        except LookupError:
            return raw_country
        if len(r) == 0:
            return raw_country

        return r[0].name

    self = get_countries(self)
    other = get_countries(other)

    return self == other


@lru_cache()
def phonetic_distance(self: str, other: str) -> float:
    """
    :param self: The first string to compare
    :param other: The second string to compare

    :return: A float between 0 and 1 representing similarity of self and other.
    """
    max_length = max(len(self), len(other))
    if max_length == 0:
        return 0

    d = distance(self, other)
    return 1 - (d / max_length)


@lru_cache()
def compare_stripped_numbers(self: str, other: str) -> float:
    """
    :param self: The first string to compare
    :param other: The second string to compare

    :return: A float between 0 and 1 representing similarity of self and other.

    strip all non-numeric characters, and do a comparison
    """
    return ''.join(filter(str.isdigit, self)) == ''.join(filter(str.isdigit, other))


@lru_cache()
def compare_url(self: str, other: str) -> float:
    """
    :param self: The first string to compare
    :param other: The second string to compare

    :return: A float between 0 and 1 representing similarity of self and other.
    """
    self = self.replace("/", "").replace("www.", "").replace("http:", "").replace("https:", "")
    other = other.replace("/", "").replace("www.", "").replace("http:", "").replace("https:", "")
    return phonetic_distance(self, other)


FIELD_ALGORITHMS: Dict[Algorithm, Callable[[Any, Any], float]] = {
    Algorithm.EQUALITY: lambda self, other: self == other,
    Algorithm.PHONETIC_DISTANCE: phonetic_distance,
    Algorithm.URL: compare_url,
    Algorithm.COUNTRY: compare_country,
    Algorithm.VAT_ID: compare_stripped_numbers,
    Algorithm.PHONE: compare_stripped_numbers,
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
