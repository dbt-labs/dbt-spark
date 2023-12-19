SCHEMA_YML = """version: 2
sources:
  - name: test_source
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
    schema: "{{ env_var('DBT_GET_LAST_RELATION_TEST_SCHEMA') }}"
    tables:
      - name: test_source
"""

SEED_TEST_SOURCE_CSV = """
id,name
1,Martin
2,Jeter
3,Ruth
4,Gehrig
5,DiMaggio
6,Torre
7,Mantle
8,Berra
9,Maris
""".strip()
