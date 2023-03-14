expected_append_csv = """
id,msg
1,hello
2,goodbye
2,yo
3,anyway
""".lstrip()

expected_overwrite_csv = """
id,msg
2,yo
3,anyway
""".lstrip()

expected_partial_upsert_csv = """
id,msg,color
1,hello,blue
2,yo,red
3,anyway,purple
""".lstrip()

expected_upsert_csv = """
id,msg
1,hello
2,yo
3,anyway
""".lstrip()
