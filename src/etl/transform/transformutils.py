"""Transform specific utils"""

# pylint: disable=invalid-name
import logging
import os
from datetime import datetime
from typing import Final

from sqlalchemy import text

from ..context import ETLContext

logger = logging.getLogger(__name__)


DATE_FORMAT: Final[str] = "%Y-%m-%d"


def execute_sql_transform(ctxt: ETLContext, sql: str) -> None:
    """Execute sql for a given session"""
    with ctxt.transaction() as cnxn:
        cnxn.execute(text(sql))


def execute_sql_file(ctxt: ETLContext, filename: str, encoding="utf-8") -> None:
    """Execute SQL given a filename containing the SQL statements"""
    parent_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
    with open(f"{parent_dir}/sql/{filename}", "r", encoding=encoding) as fsql:
        sql_statement = fsql.read()
        execute_sql_transform(ctxt, sql_statement)


def try_parsing_date(input_date: str) -> str:
    date_formats = [
        "%d/%m/%y",
        "%d/%m/%Y",
        "%d-%m-%y",
        "%d-%m-%Y",
        "%Y-%m-%d",
        "%y-%m-%d",
        "%d/%m/%y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%y-%m-%d %H:%M:%S",
        "%d/%m/%y %H:%M",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d %H:%M",
        "%y-%m-%d %H:%M",
    ]

    for _format in date_formats:
        try:
            parsed_date = datetime.strptime(str(input_date), _format)
            return parsed_date.strftime(DATE_FORMAT)
        except ValueError:
            continue

    return input_date
