import pandas as pd
import numpy as np
from io import BytesIO
import pyreadstat
import os
import tempfile


class SourceFilesUtilities:
    
    def load_dataframe(self, content, content_type, filename):
        if content_type == "text/csv" or filename.lower().endswith(".csv"):
            return pd.read_csv(BytesIO(content)), {}
        elif content_type in (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-excel"
        ) or filename.lower().endswith((".xls", ".xlsx")):
            return pd.read_excel(BytesIO(content)), {}
        elif content_type == "application/x-sas-data" or filename.lower().endswith(".sas7bdat"):
            with tempfile.NamedTemporaryFile(delete=False, suffix=".sas7bdat") as tmp:
                tmp.write(content)
                tmp_path = tmp.name

            try:
                df, meta = pyreadstat.read_sas7bdat(tmp_path)
                col_descriptions = dict(zip(meta.column_names, meta.column_labels))
                return df, col_descriptions
            finally:
                os.remove(tmp_path)
        else:
            raise ValueError(f"Unsupported content type or file extension: {content_type}")


    def infer_data_type(self, series):
        s = series.dropna()
        if s.empty:
            return "string?"

        if pd.api.types.is_integer_dtype(series):
            return "int?" if series.isnull().any() else "int"

        if pd.api.types.is_float_dtype(series):
            s_f = s.astype(float)
            if np.isclose(s_f % 1, 0).all():
                return "int?" if series.isnull().any() else "int"
            return "float"

        if pd.api.types.is_bool_dtype(series):
            return "bool"

        if pd.api.types.is_datetime64_any_dtype(series):
            return "datetime"

        return "string"
    

    def canon_dtype(self, dt):
        d = (dt or "").lower().strip()
        nullable = False
        if d.endswith("?"):
            d, nullable = d[:-1], True

        # accept either our canonical tags or pandas-style leftovers defensively
        mapping = {
            "int64": "int", "int32": "int", "integer": "int", "int": "int",
            "float64": "float", "float32": "float", "float": "float", "double": "float", "decimal": "float",
            "bool": "bool", "boolean": "bool",
            "datetime64[ns]": "datetime", "datetime": "datetime", "timestamp": "datetime", "date": "datetime",
            "object": "string", "text": "string", "string": "string"
        }
        base = mapping.get(d, d)
        return base, nullable
    

    def coerce_for_stats(self, series, logical_dtype: str | None):
        """Use your canon dtype to coerce for consistent stats."""
        base, _ = self.canon_dtype(logical_dtype)
        if base in {"int", "float"}:
            return pd.to_numeric(series, errors="coerce")
        if base == "datetime":
            return pd.to_datetime(series, errors="coerce", utc=False)
        if base == "bool":
            # glide shows booleans nicely; keep as-is but coerce common strings
            s = series.astype("string").str.strip().str.lower()
            mapping = {"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False}
            return s.map(mapping).astype("boolean")
        # default string
        return series.astype("string")


    def compute_stats(self, series: pd.Series, total_rows: int, logical_dtype: str | None, top_k: int = 3):
        """One stats engine used by both data paths."""
        s = self.coerce_for_stats(series, logical_dtype)
        non_null = s.dropna()
        nulls = int(total_rows - len(non_null))
        pct_nulls = (nulls / total_rows * 100.0) if total_rows else 0.0

        vc = non_null.value_counts().head(top_k)
        top = [
            {"value": ("" if pd.isna(k) else str(k)), "count": int(v),
            "pct": (int(v) / total_rows * 100.0) if total_rows else 0.0}
            for k, v in vc.items()
        ]

        stats = {
            "nulls": nulls,
            "pct_nulls": pct_nulls,
            "distinct": int(non_null.nunique()),
            "top": top,
        }

        base, _ = self.canon_dtype(logical_dtype)
        if base in {"int", "float", "datetime"} and len(non_null) > 0:
            try:
                if base == "datetime":
                    # describe ranges; mean not very meaningful here
                    stats.update({
                        "min": pd.to_datetime(non_null).min().isoformat(),
                        "max": pd.to_datetime(non_null).max().isoformat(),
                        "p25": None, "p50": None, "p75": None, "mean": None,
                    })
                else:
                    q = non_null.quantile([0.25, 0.5, 0.75])
                    stats.update({
                        "min": float(non_null.min()),
                        "max": float(non_null.max()),
                        "mean": float(non_null.mean()),
                        "p25": float(q.get(0.25)),
                        "p50": float(q.get(0.5)),
                        "p75": float(q.get(0.75)),
                    })
            except Exception:
                # be forgiving on weird columns
                pass

        return stats




    