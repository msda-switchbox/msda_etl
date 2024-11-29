"""Util functions for transform tests"""


def get_sql_str_list(call_args_list: list):
    """Transform mock call_args_list text objects to list of strings"""
    sql_str_list = []
    for i in call_args_list:
        args_list = [str(arg) for arg in i.args]
        sql_str_list += args_list
    return sql_str_list
