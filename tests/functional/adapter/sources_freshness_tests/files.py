SCHEMA_YML = """version: 2
sources:
  - name: test_source
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
    schema: "{{ env_var('DBT_GET_LAST_RELATION_TEST_SCHEMA') }}"
    tables:
      - name: test_source_no_last_modified
      - name: test_source_last_modified
        loaded_at_field: last_modified
"""

SEED_TEST_SOURCE_NO_LAST_MODIFIED_CSV = """
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

SEED_TEST_SOURCE_LAST_MODIFIED_CSV = """
id,name,last_modified
1,Martin,2023-01-01 00:00:00
2,Jeter,2023-02-01 00:00:00
3,Ruth,2023-03-01 00:00:00
4,Gehrig,2023-04-01 00:00:00
5,DiMaggio,2023-05-01 00:00:00
6,Torre,2023-06-01 00:00:00
7,Mantle,2023-07-01 00:00:00
8,Berra,2023-08-01 00:00:00
9,Maris,2023-09-01 00:00:00
""".strip()
