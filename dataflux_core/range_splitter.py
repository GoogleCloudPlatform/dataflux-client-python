"""
 Copyright 2023 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 """

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from fractions import Fraction
from itertools import count


@dataclass
class MinimalIntRange:
    start_int: int
    end_int: int
    min_len: int


@dataclass
class GenerateSplitsOpts:
    min_int_range: MinimalIntRange
    num_splits: int
    start_range: str
    end_range: str


class RangeSplitter(object):
    """Manages splits performed to facilitate the work-stealing algorithm.

    Attr:
      alphabet_map: An int mapping for an alphabet of arbitrary character size.
      sorted_alphabet: The sorted alphabet that initializes the RangeSplitter.
    """

    min_splits = 2

    def __init__(self, alphabet_map: dict[int, str],
                 sorted_alphabet: Sequence[str]):
        self.alphabet_map = alphabet_map
        self.sorted_alphabet = sorted_alphabet
        self.alphabet_set = set(sorted_alphabet)

    def split_range(
        self,
        start_range: str,
        end_range: str,
        num_splits: int,
    ) -> Sequence[str]:
        """Creates a given number of splits based on a provided start and end range.

        Args:
          start_range (str): The string marking the start of the split range.
          end_range (str): The string marking the end of the split range.
          num_splits (int): The number of splitpoints to return.

        Returns:
          A sequence of split points dividing up the provided range.
        """
        if num_splits < 1:
            raise ValueError("Got num_splits of %s but need minimum of %s." %
                             (num_splits, self.min_splits))
        if len(end_range) != 0 and start_range >= end_range:
            return []

        if self.is_range_equal_with_padding(start_range, end_range):
            return []

        self.add_characters_to_alphabet(start_range + end_range)

        min_int_range = self.string_to_minimal_int_range(
            start_range, end_range, num_splits)

        split_points = self.generate_splits(
            GenerateSplitsOpts(min_int_range, num_splits, start_range,
                               end_range))
        return split_points

    def generate_splits(self, opts: GenerateSplitsOpts) -> Sequence[str]:
        """Generates a list of split points.

        Args:
          opts (GenerateSplitOpts): Set of options for generating splitpoints

        Returns:
          A list of split points.
        """
        start_int = opts.min_int_range.start_int
        end_int = opts.min_int_range.end_int
        min_len = opts.min_int_range.min_len

        range_diff = end_int - start_int
        split_points = []
        range_interval = opts.num_splits + 1
        adjustment = Fraction(range_diff / range_interval)

        for i in range(1, opts.num_splits + 1):
            split_point = start_int + adjustment * i
            split_string = self.int_to_string(int(split_point), min_len)

            is_greater_than_start = (len(split_string) > 0
                                     and split_string > opts.start_range)
            is_less_than_end = len(
                opts.end_range) == 0 or (len(split_string) > 0
                                         and split_string < opts.end_range)

            if is_greater_than_start and is_less_than_end:
                split_points.append(split_string)

        return split_points

    def int_to_string(self, split_point: int, string_len: int) -> str:
        """Converts the base len(alphabet) int back into a string.

        Args:
          split_point (int): A valid split point int to be converted to string.
          string_len (int): The required length of the resulting string.

        Returns:
          A string derived from a base len(alphabet) int.
        """
        alphabet_len = len(self.sorted_alphabet)
        split_string = ""

        for _ in range(string_len):
            remainder = split_point % alphabet_len
            split_point //= alphabet_len
            split_string += self.sorted_alphabet[remainder]

        # This is assembeled backwards via division, so we reverse the final string.
        return split_string[::-1]

    def string_to_minimal_int_range(self, start_range: str, end_range: str,
                                    num_splits: int) -> MinimalIntRange:
        """Converts a string range to a minimal integer range.

        Args:
          start_range (str): The string marking the start of the split range.
          end_range (str): The string marking the end of the split range.
          num_splits (int): The number of splitpoints to return.

        Returns:
          A minimal integer range.
        """

        start_int = 0
        end_int = 0

        alphabet_len = len(self.sorted_alphabet)
        start_char = self.sorted_alphabet[0]
        end_char = self.sorted_alphabet[-1]

        end_default_char = start_char
        if len(end_range) == 0:
            end_default_char = end_char

        for i in count(0):
            start_pos = self.alphabet_map[get_char_or_default(
                start_range, i, start_char)]
            start_int *= alphabet_len
            start_int += start_pos

            end_pos = self.alphabet_map[get_char_or_default(
                end_range, i, end_default_char)]
            end_int *= alphabet_len
            end_int += end_pos

            difference = end_int - start_int
            if difference > num_splits:
                # Due to zero indexing, min length must have 1 added to it.
                return MinimalIntRange(start_int, end_int, i + 1)

    def is_range_equal_with_padding(self, start_range: str, end_range: str):
        """Checks for equality between two string ranges.

        Args:
          start_range (str): The start range for the split.
          end_range (str): The end range for the split.

        Returns:
          Boolean indicating equality of the two provided ranges.
        """

        if len(end_range) == 0:
            return False

        longest = max(len(start_range), len(end_range))

        smallest_char = self.sorted_alphabet[0]

        for i in range(longest):
            char_start = get_char_or_default(start_range, i, smallest_char)
            char_end = get_char_or_default(end_range, i, smallest_char)

            if char_start != char_end:
                return False

        return True

    def add_characters_to_alphabet(self, characters: str):
        """Adds a character to the known alphabet.

        Args:
          characters: The string of characters to add to the library.
        """
        unique_characters = set(characters)
        new_alphabet = self.alphabet_set.union(unique_characters)
        if len(new_alphabet) != len(self.alphabet_set):
            self.sorted_alphabet = sorted(new_alphabet)
            self.alphabet_map = {
                val: index
                for index, val in enumerate(self.sorted_alphabet)
            }


def get_char_or_default(characters: str, index: int, default_char: str) -> str:
    """Returns the character at the given index or the default character if the index is out of bounds.

    Args:
      characters (str): The range string to check.
      index (int): The current iteration index across characters.
      default_char (str): The smallest character in the implemented char set.

    Returns:
      The resulting character for the given index.
    """
    if index < 0 or index >= len(characters):
        return default_char

    return characters[index]


def new_rangesplitter(alphabet: str) -> RangeSplitter:
    """Creates a new RangeSplitter with the given alphabets.

    Note that the alphabets are a predetermined set of characters
    by the work-stealing algorithm, and the characters are guaranteed to be unique.

    Args:
      alphabet (str): The full set of characters used for this range splitter.

    Returns:
      An instance of the RangeSplitter class that is used to manage splits
      performed to facilitate the work-stealing algorithm.
    """
    if len(alphabet) == 0:
        raise ValueError("Cannot split with an empty alphabet.")
    sorted_alphabet = sorted(alphabet)
    alphabet_map = {val: index for index, val in enumerate(sorted_alphabet)}
    return RangeSplitter(alphabet_map, sorted_alphabet)
