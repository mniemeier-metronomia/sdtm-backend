import pandas as pd
from sqlalchemy import select, distinct, asc
from db import Session, SourceData, SourceColumn
from mapper.transformer_utils import TransformerUtils
from mapper.modifications.modifications_service import ModificationsService
from mapper.modifications.translate import ui_mods_to_server_ops
from source_files.source_files_utilities import SourceFilesUtilities
import re

class PreviewModsService:
    def __init__(self):
        self.utils = TransformerUtils()
        self.mods = ModificationsService()
        self.sfutils = SourceFilesUtilities()

    # ------------ helpers -----------------

    def _list_where_fields(self, where):
        if not isinstance(where, dict): return []
        t = (where.get("type") or "").lower()
        if t == "group":
            out = []
            for ch in where.get("children") or []:
                out.extend(self._list_where_fields(ch))
            return out
        if t == "rule":
            return [where.get("field")] if where.get("field") else []
        return []

    def _needed_cols_from_assign(self, assign):
        mode = (assign.get("mode") or "static").lower()
        val = assign.get("value") or ""
        cols = set()
        if mode == "column":
            if val: cols.add(str(val))
        elif mode == "expression":
            # tokens like {COL}
            for token in re.findall(r"\{([A-Za-z_][A-Za-z0-9_]*)\}", val):
                cols.add(token)
            # (optional) bare identifiers for df.eval expressions
            for token in re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", val):
                cols.add(token)
        # fallback may reference a column or expression too
        fb = assign.get("fallback")
        if isinstance(fb, dict):
            fmode = (fb.get("mode") or "").lower()
            fval = fb.get("value") or ""
            if fmode == "column" and fval:
                cols.add(str(fval))
            elif fmode == "expression":
                for token in re.findall(r"\{([A-Za-z_][A-Za-z0-9_]*)\}", fval):
                    cols.add(token)
        return list(cols)

    def _col_types_map(self, session, source_file_id: int):
        rows = (
            session.query(SourceColumn.name, SourceColumn.data_type)
            .filter_by(source_file_id=source_file_id)
            .all()
        )
        return {name: dtype for (name, dtype) in rows}

    def _load_source_subset(self, session, source_file_id: int, needed_cols, max_rows: int) -> pd.DataFrame:
        # sample row_index deterministically
        row_idx_stmt = (
            select(distinct(SourceData.row_index))
            .where(SourceData.source_file_id == source_file_id)
            .order_by(asc(SourceData.row_index))
            .limit(max_rows)
        )
        idx = [r[0] for r in session.execute(row_idx_stmt).all()]
        if not idx:
            return pd.DataFrame(columns=needed_cols or [])

        if not needed_cols:
            return pd.DataFrame(index=idx)

        long_stmt = (
            select(SourceData.row_index, SourceData.column_name, SourceData.value)
            .where(
                SourceData.source_file_id == source_file_id,
                SourceData.column_name.in_(needed_cols),
                SourceData.row_index.in_(idx),
            )
        )
        rows = session.execute(long_stmt).all()

        by_index = {ri: {} for ri in idx}
        for ri, col, val in rows:
            by_index.setdefault(ri, {})[col] = val

        wide = pd.DataFrame.from_dict(by_index, orient="index").reindex(index=idx)
        for c in needed_cols:
            if c not in wide.columns:
                wide[c] = pd.Series([None] * len(wide), index=wide.index)
        return wide[needed_cols]

    def _top_counts(self, s: pd.Series, k: int = 20):
        vc = s.value_counts(dropna=False).head(k)
        total = len(s)
        out = []
        for val, cnt in vc.items():
            is_na = pd.isna(val)
            out.append({
                "value": None if is_na else val,
                "display": "" if is_na else str(val),
                "count": int(cnt),
                "pct": round((cnt / total * 100.0), 2) if total else 0.0,
            })
        return out

    def _sample_pairs(self, orig: pd.Series, trans: pd.Series, k: int = 25):
        df = pd.DataFrame({"original": orig, "transformed": trans})
        df["original_display"] = df["original"].astype("string")
        df["transformed_display"] = df["transformed"].astype("string")
        unique = df.drop_duplicates(subset=["original_display", "transformed_display"]).head(k)
        return [
            {
                "original": None if pd.isna(r.original) else r.original,
                "original_display": "" if pd.isna(r.original) else r.original_display,
                "transformed": None if pd.isna(r.transformed) else r.transformed,
                "transformed_display": "" if pd.isna(r.transformed) else r.transformed_display,
            }
            for _, r in unique.iterrows()
        ]

    # ------------ main ---------------------

    def preview_assign_modifications(self, source_file_id: int, assign: dict, mods: list,
                                     where: dict | None = None, top_n: int = 20, max_rows: int = 5000):
        session = Session()
        try:
            # figure out which source columns we need (assign + where)
            assigns_cols = self._needed_cols_from_assign(assign)
            where_cols = self._list_where_fields(where) if where else []
            needed_cols = sorted({*assigns_cols, *[c for c in where_cols if c]})

            df = self._load_source_subset(session, source_file_id, needed_cols, max_rows)

            # type map for where coercion
            col_types = self._col_types_map(session, source_file_id)

            # filter by WHERE (over df as loaded)
            mask = self.utils.eval_where(df, where, col_types=col_types) if where else pd.Series(True, index=df.index)
            if mask.sum() == 0:
                return {"original_top": [], "transformed_top": [], "samples": [], "row_count": 0}

            df_sub = df[mask]

            # build base series from assign (raw, unmodified)
            base = self.utils.eval_assign_series(df_sub, assign)
            fb = assign.get("fallback")
            if fb:
                base = self.utils.apply_fallback(base, df_sub, fb)

            ops, ignored = ui_mods_to_server_ops(mods)
            out = self.mods.apply(base, ops)

            return {
                "original_top": self._top_counts(base, k=top_n),
                "transformed_top": self._top_counts(out, k=top_n),
                "samples": self._sample_pairs(base, out, k=min(50, top_n + 5)),
                "row_count": int(len(base)),
                "ignored_mods": ignored,
            }
        finally:
            session.close()
