"""Module for database utilities and helpers"""

import logging
from contextlib import contextmanager
from enum import Enum
from tempfile import SpooledTemporaryFile
from typing import Generator, Iterable, Literal, Optional

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class WriteMode(Enum):
    """Enum for write modes"""

    APPEND = 1
    OVERWRITE = 2


# pylint: disable=too-many-arguments
def df_to_sql(
    cnxn: Connection,
    dataframe: pd.DataFrame,
    table: str,
    columns: Optional[Iterable[str]] = None,
    encoding: Optional[str] = "utf-8",
    delimiter: Optional[str] = ";",
    null_field: Optional[str] = None,
    write_mode: Optional[
        Literal[WriteMode.APPEND, WriteMode.OVERWRITE]
    ] = WriteMode.OVERWRITE,
):
    """
    Helper function to quickly copy a Pandas DataFrame to an
    existing table in the database. All rows in the table are
    deleted before the copy.
    """
    read_buffer_size: int = 8192
    write_buffer_size: int = 268435500

    if not dataframe.empty:
        with SpooledTemporaryFile(
            max_size=write_buffer_size,
            mode="w+t",
            encoding=encoding,
        ) as csv_buffer:
            # take all columns by default
            if columns is None:
                columns = dataframe.columns

            dataframe[columns].to_csv(
                csv_buffer,
                delimiter,
                header=False,
                index=False,
                encoding=encoding,
            )

            quote = '"'
            options = [
                "FORMAT CSV",
                f"DELIMITER E'{delimiter}'",
                "HEADER FALSE",
                f"QUOTE E'{quote}'",
            ]
            if null_field is not None:
                options.append(f"NULL '{null_field}'")
            options_str = ", ".join(options)

            cols = ",".join([f'"{c}"' for c in columns])
            copy_query = (
                f"COPY {table} ({cols}) FROM STDIN WITH ({options_str})".strip()
            )
            csv_buffer.seek(0)
            with cnxn.connection.cursor() as cursor:
                if write_mode == WriteMode.OVERWRITE:
                    cursor.execute(f"DELETE FROM {table};")
                cursor.copy_expert(copy_query, csv_buffer, read_buffer_size)


@contextmanager
def session_context(
    session: Session,
) -> Generator[Session, None, None]:
    """Context manager for using a database session,
    given a specific engine implementation"""
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def create_engine_from_args(
    dbms: str,
    host: str,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    dbname: Optional[str] = None,
    schema: Optional[str] = None,
    **kwargs,
) -> Engine:
    """return a sqlalchemy engine for the given configuration"""
    if dbms in ("postgres", "postgresql"):
        url = f"postgresql://{username}:{password}@{host}:{port}/{dbname}"
        args = {"options": f"-csearch_path={schema}"} if schema else None
        return create_engine(url, connect_args=args, **kwargs)
    if dbms in ("sqlite",):
        return create_engine(f"sqlite:///{host}", **kwargs)
    raise ValueError(f"unsupported DBMS: {dbms}")
