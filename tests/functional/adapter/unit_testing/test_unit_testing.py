import pytest

from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes
from dbt.tests.adapter.unit_testing.test_case_insensitivity import BaseUnitTestCaseInsensivity
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput


class TestSparkUnitTestingTypes(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["2.0", "2.0"],
            ["'12345'", "12345"],
            ["'string'", "string"],
            ["true", "true"],
            ["date '2011-11-11'", "2011-11-11"],
            ["timestamp '2013-11-03 00:00:00-0'", "2013-11-03 00:00:00-0"],
            # ["map(struct('Hello', 'World'), 'Greeting')", '''"map(struct('Hello', 'World'), 'Greeting')"'''],
            # ['named_struct("a", 1, "b", 2, "c", 3)', """'named_struct("a", 1, "b", 2, "c", 3)'"""],
            # ["array(1, 2, 3)", "'array(1, 2, 3)'"],
        ]


class TestSparkUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass


class TestSparkUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass
