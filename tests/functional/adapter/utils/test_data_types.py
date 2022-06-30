import pytest
from dbt.tests.adapter.utils.data_types.test_type_bigint import BaseTypeBigInt
from dbt.tests.adapter.utils.data_types.test_type_float import BaseTypeFloat
from dbt.tests.adapter.utils.data_types.test_type_int import BaseTypeInt
from dbt.tests.adapter.utils.data_types.test_type_numeric import BaseTypeNumeric
from dbt.tests.adapter.utils.data_types.test_type_string import BaseTypeString
from dbt.tests.adapter.utils.data_types.test_type_timestamp import BaseTypeTimestamp


class TestTypeBigInt(BaseTypeBigInt):
    pass

    
class TestTypeFloat(BaseTypeFloat):
    pass

    
class TestTypeInt(BaseTypeInt):
    pass

    
class TestTypeNumeric(BaseTypeNumeric):
    pass

    
class TestTypeString(BaseTypeString):
    pass

    
class TestTypeTimestamp(BaseTypeTimestamp):
    pass
