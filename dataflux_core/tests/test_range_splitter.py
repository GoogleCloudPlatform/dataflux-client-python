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

from dataflux_core import range_splitter
import unittest


class RangeSplitterTest(unittest.TestCase):
    def test_range_splits(self):
        test_cases = [
            {
                "desc": "less than one split",
                "start": "1",
                "end": "2",
                "splits": 0,
                "expected_error": ValueError,
            },
            {
                "desc": "end smaller than start range",
                "start": "456",
                "end": "123",
                "splits": 1,
                "expected_error": None,
                "result": [],
            },
            {
                "desc": "start and end equal after padding",
                "start": "9",
                "end": "90",
                "splits": 100,
                "expected_error": None,
                "result": [],
            },
            {
                "desc": "tight range split",
                "start": "199999",
                "end": "2",
                "splits": 1,
                "expected_error": None,
                "result": ["1999995"],
            },
            {
                "desc": "split full namespace",
                "start": "",
                "end": "",
                "splits": 24,
                "expected_error": None,
                "result": [
                    "03",
                    "07",
                    "11",
                    "15",
                    "19",
                    "23",
                    "27",
                    "31",
                    "35",
                    "39",
                    "43",
                    "47",
                    "51",
                    "55",
                    "59",
                    "63",
                    "67",
                    "71",
                    "75",
                    "79",
                    "83",
                    "87",
                    "91",
                    "95",
                ],
            },
            {
                "desc": "split with only start range",
                "start": "5555",
                "end": "",
                "splits": 4,
                "expected_error": None,
                "result": ["63", "72", "81", "90"],
            },
            {
                "desc": "large gap small number of splits",
                "start": "0",
                "end": "9",
                "splits": 3,
                "expected_error": None,
                "result": ["2", "4", "6"],
            },
            {
                "desc": "split with longer prefix",
                "start": "0123455111",
                "end": "012347",
                "splits": 1,
                "expected_error": None,
                "result": ["012346"],
            },
            {
                "desc": "split with only end range",
                "start": "",
                "end": "9",
                "splits": 1,
                "expected_error": None,
                "result": ["4"],
            },
        ]
        rs = range_splitter.new_rangesplitter("0123456789")
        for tc in test_cases:
            try:
                result = rs.split_range(tc["start"], tc["end"], tc["splits"])
                self.assertEqual(result, tc["result"])
            except tc["expected_error"]:
                pass

    def test_add_characters_to_alphabet(self):
        test_cases = [
            {
                "desc": "empty strings",
                "chars": "",
                "expected_alphabet_map": {"7": 0, "8": 1, "9": 2},
            },
            {
                "desc": "no new characters",
                "chars": "998",
                "expected_alphabet_map": {"7": 0, "8": 1, "9": 2},
            },
            {
                "desc": "new characters",
                "chars": "102",
                "expected_alphabet_map": {
                    "0": 0,
                    "1": 1,
                    "2": 2,
                    "7": 3,
                    "8": 4,
                    "9": 5,
                },
            },
        ]
        rs = range_splitter.new_rangesplitter("789")

        for tc in test_cases:
            rs.add_characters_to_alphabet(tc["chars"])
            self.assertEqual(rs.alphabet_map, tc["expected_alphabet_map"], tc["desc"])

    def test_int_to_string(self):
        test_cases = [
            {
                "desc": "get a string",
                "split_point": 15,
                "string_len": 3,
                "result": "023",
            },
            {
                "desc": "max number",
                "split_point": 215,
                "string_len": 3,
                "result": "BBB",
            },
            {
                "desc": "large than max number",
                "split_point": 220,
                "string_len": 3,
                "result": "00A",
            },
        ]
        rs = range_splitter.new_rangesplitter("0123AB")
        for tc in test_cases:
            result = rs.int_to_string(tc["split_point"], tc["string_len"])
            self.assertEqual(result, tc["result"], tc["desc"])

    def test_int_to_string_empty_range(self):
        test_cases = [
            {
                "desc": "get a string",
                "split_point": 9,
                "string_len": 3,
                "result": "",
                "expected_error": ValueError,
            },
        ]
        for tc in test_cases:
            try:
                rs = range_splitter.new_rangesplitter("")
                result = rs.int_to_string(tc["split_point"], tc["string_len"])
                self.assertEqual(result, tc["result"], tc["desc"])
            except tc["expected_error"]:
                pass

    def test_get_char_or_default(self):
        test_cases = [
            {
                "desc": "index larger than character string",
                "characters": "15",
                "index": 3,
                "default_char": "0",
                "result": "0",
            },
            {
                "desc": "index in string length",
                "characters": "15ABC",
                "index": 2,
                "default_char": "0",
                "result": "A",
            },
            {
                "desc": "index less than 0",
                "characters": "15ABC",
                "index": -3,
                "default_char": "0",
                "result": "0",
            },
            {
                "desc": "empty character",
                "characters": "",
                "index": 1,
                "default_char": "0",
                "result": "0",
            },
        ]
        for tc in test_cases:
            result = range_splitter.get_char_or_default(
                tc["characters"], tc["index"], tc["default_char"]
            )
            self.assertEqual(result, tc["result"], tc["desc"])

    def test_is_range_equal_with_padding(self):
        test_cases = [
            {
                "desc": "start and end range with padding are equal",
                "start": "15",
                "end": "1500",
                "result": True,
            },
            {
                "desc": "start and end range with padding are not equal",
                "start": "15",
                "end": "150A",
                "result": False,
            },
            {
                "desc": "end range is empty",
                "start": "15",
                "end": "",
                "result": False,
            },
            {
                "desc": "start range is empty",
                "start": "",
                "end": "09",
                "result": False,
            },
            {
                "desc": "start range is empty",
                "start": "",
                "end": "0",
                "result": True,
            },
            {
                "desc": "start and end range are empty",
                "start": "",
                "end": "",
                "result": False,
            },
            {
                "desc": "start and end range are not equal",
                "start": "21",
                "end": "12",
                "result": False,
            },
            {
                "desc": "start and end range are equal",
                "start": "21",
                "end": "21",
                "result": True,
            },
        ]
        rs = range_splitter.new_rangesplitter("01A")
        for tc in test_cases:
            result = rs.is_range_equal_with_padding(tc["start"], tc["end"])
            self.assertEqual(result, tc["result"], tc["desc"])

    def test_string_to_minimal_int_range(self):
        test_cases = [
            {
                "desc": "split numbers",
                "start": "00",
                "end": "20",
                "splits": 3,
                "result": range_splitter.MinimalIntRange(
                    start_int=0, end_int=20, min_len=2
                ),
            },
            {
                "desc": "start is non-zero",
                "start": "06",
                "end": "201",
                "splits": 4,
                "result": range_splitter.MinimalIntRange(
                    start_int=6, end_int=20, min_len=2
                ),
            },
            {
                "desc": "start with smaller suffix",
                "start": "091",
                "end": "10",
                "splits": 2,
                "result": range_splitter.MinimalIntRange(
                    start_int=91, end_int=100, min_len=3
                ),
            },
            {
                "desc": "start is empty",
                "start": "",
                "end": "10",
                "splits": 2,
                "result": range_splitter.MinimalIntRange(
                    start_int=0, end_int=10, min_len=2
                ),
            },
            {
                "desc": "start and end are empty",
                "start": "",
                "end": "",
                "splits": 24,
                "result": range_splitter.MinimalIntRange(
                    start_int=0, end_int=99, min_len=2
                ),
            },
            {
                "desc": "end is empty",
                "start": "5555",
                "end": "",
                "splits": 4,
                "result": range_splitter.MinimalIntRange(
                    start_int=55, end_int=99, min_len=2
                ),
            },
            {
                "desc": "tight range split",
                "start": "199999",
                "end": "2",
                "splits": 1,
                "result": range_splitter.MinimalIntRange(
                    start_int=1999990, end_int=2000000, min_len=7
                ),
            },
            {
                "desc": "tight range split",
                "start": "8100",
                "end": "9100",
                "splits": 3,
                "result": range_splitter.MinimalIntRange(
                    start_int=81, end_int=91, min_len=2
                ),
            },
        ]
        rs = range_splitter.new_rangesplitter("0123456789")
        for tc in test_cases:
            result = rs.string_to_minimal_int_range(
                tc["start"], tc["end"], tc["splits"]
            )
            self.assertEqual(result, tc["result"], tc["desc"])

    def test_generate_splits(self):
        test_cases = [
            {
                "desc": "less than one split",
                "start": "1",
                "end": "2",
                "splits": 0,
                "result": [],
            },
            {
                "desc": "tight range split",
                "start": "199999",
                "end": "2",
                "splits": 1,
                "result": ["1999995"],
            },
            {
                "desc": "split full namespace",
                "start": "",
                "end": "",
                "splits": 24,
                "result": [
                    "03",
                    "07",
                    "11",
                    "15",
                    "19",
                    "23",
                    "27",
                    "31",
                    "35",
                    "39",
                    "43",
                    "47",
                    "51",
                    "55",
                    "59",
                    "63",
                    "67",
                    "71",
                    "75",
                    "79",
                    "83",
                    "87",
                    "91",
                    "95",
                ],
            },
            {
                "desc": "split with only start range",
                "start": "5555",
                "end": "",
                "splits": 4,
                "result": ["63", "72", "81", "90"],
            },
            {
                "desc": "large gap small number of splits",
                "start": "0",
                "end": "9",
                "splits": 3,
                "result": ["2", "4", "6"],
            },
            {
                "desc": "split with longer prefix",
                "start": "0123455111",
                "end": "012347",
                "splits": 1,
                "result": ["012346"],
            },
            {
                "desc": "split with only end range",
                "start": "",
                "end": "9",
                "splits": 1,
                "result": ["4"],
            },
            {
                "desc": "tight range split",
                "start": "8100",
                "end": "9100",
                "splits": 3,
                "result": ["83", "86", "88"],
            },
        ]
        rs = range_splitter.new_rangesplitter("0123456789")
        for tc in test_cases:
            min_int_range = rs.string_to_minimal_int_range(
                tc["start"], tc["end"], tc["splits"]
            )
            opts = range_splitter.GenerateSplitsOpts(
                min_int_range, tc["splits"], tc["start"], tc["end"]
            )
            result = rs.generate_splits(opts)
            self.assertEqual(result, tc["result"], tc["desc"])


if __name__ == "__main__":
    unittest.main()
