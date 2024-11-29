"""A module for sql helpers"""

import os
from typing import Final

DATE_FORMAT: Final[str] = os.environ.get("DATE_FORMAT", "DDMONYYYY")


def cast_date_format() -> str:
    return """
    create or replace function cast_date(in text)
    returns date
    as
    $$
    begin
    begin
        return $1::date;
    exception
        when others then
        return null;
    end;
    end;
    $$
    language plpgsql;
    """
