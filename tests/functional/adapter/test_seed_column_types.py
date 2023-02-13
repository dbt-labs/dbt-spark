import pytest
from dbt.tests.util import run_dbt

_SEED_CSV = """
ID,ORDERID,PAYMENTMETHOD,STATUS,AMOUNT,AMOUNT_USD,CREATED
1,1,credit_card,success,1000,10.00,2018-01-01
2,2,credit_card,success,2000,20.00,2018-01-02
3,3,coupon,success,100,1.00,2018-01-04
4,4,coupon,success,2500,25.00,2018-01-05
5,5,bank_transfer,fail,1700,17.00,2018-01-05
6,5,bank_transfer,success,1700,17.00,2018-01-05
7,6,credit_card,success,600,6.00,2018-01-07
8,7,credit_card,success,1600,16.00,2018-01-09
9,8,credit_card,success,2300,23.00,2018-01-11
10,9,gift_card,success,2300,23.00,2018-01-12
"""

_SEED_YML = """
seeds:
  - name: payments
    config:
        column_types:
            id: string
            orderid: string
            paymentmethod: string
            status: string
            amount: int
            amount_usd: decimal(20,2)
            created: timestamp
"""

@pytest.mark.skip_profile('spark_session')
class TestSeedColumnTypesCast:

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "payments.csv": _SEED_CSV,
            "seed.yml": _SEED_YML
        }

    def run_and_test(self):
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt(["test"])
        assert len(results) == 1

    #  We want to test seed types because hive would cause all fields to be strings.
    # setting column_types in project.yml should change them and pass.
    def test_column_seed_type(self):
        self.run_and_test()
