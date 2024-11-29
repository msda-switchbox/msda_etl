"""ETL SUMMARY"""

from typing import Any, List, Optional

from sqlalchemy import func, select

from ..context import ETLContext


class ModelSummary:
    """Generic summary for all models"""

    def __init__(self, model: Any) -> None:
        """init"""
        self.model = model

    def get(self, ctxt: ETLContext) -> str:
        """get the string to log"""
        qry = select(func.count()).select_from(self.model)
        result = ctxt.cnxn.execute(qry)
        return f"{self.model.__tablename__:>52}: {result.first().count_1:<20}\n"


def print_models_summary(
    ctxt: ETLContext,
    summaries: List[ModelSummary],
    title: Optional[str] = "OMOPCDM",
) -> str:
    """Print DB summary"""
    output_str = f"\n{'---':>50} {title} ---\n"
    output_str += "".join([summary.get(ctxt) for summary in summaries])
    return output_str
