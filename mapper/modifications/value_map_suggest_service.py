# services/value_map_suggest_service.py
import re
import pandas as pd
from sqlalchemy import select, distinct, asc, func, and_
from db import Session, SourceData, SourceColumn, SDTMCodelist, SDTMCodelistTerm, SDTMDomain, SDTMVariable
from mapper.transformer_utils import TransformerUtils
from source_files.source_files_utilities import SourceFilesUtilities

class ValueMapSuggestService:
    def __init__(self):
        self.utils = TransformerUtils()
        self.sfutils = SourceFilesUtilities()

    # ---------- load helpers (source) ----------
    def _list_where_fields(self, where):
        if not isinstance(where, dict): return []
        t = (where.get("type") or "").lower()
        if t == "group":
            out = []
            for ch in where.get("children") or []: out.extend(self._list_where_fields(ch))
            return out
        if t == "rule": return [where.get("field")] if where.get("field") else []
        return []

    def _needed_cols_from_assign(self, assign):
        mode = (assign.get("mode") or "static").lower()
        val = assign.get("value") or ""
        cols = set()
        if mode == "column" and val:
            cols.add(str(val))
        elif mode == "expression":
            for token in re.findall(r"\{([A-Za-z_][A-Za-z0-9_]*)\}", val):
                cols.add(token)
            for token in re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", val):
                cols.add(token)
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

    # ---------- codelist helpers ----------
    def _latest_codelist(self, session, nci_code: str):
        max_date = (
            session.query(func.max(SDTMCodelist.standard_date))
            .filter(SDTMCodelist.nci_code == nci_code)
            .scalar()
        )
        if max_date:
            return (
                session.query(SDTMCodelist)
                .filter(SDTMCodelist.nci_code == nci_code, SDTMCodelist.standard_date == max_date)
                .one_or_none()
            )
        return (
            session.query(SDTMCodelist)
            .filter(SDTMCodelist.nci_code == nci_code, SDTMCodelist.standard_date.is_(None))
            .order_by(SDTMCodelist.id.desc())
            .first()
        )

    def _resolve_codelist_code(self, session, standard_id, domain, variable, codelist_code):
        if codelist_code:
            return codelist_code
        if standard_id and domain and variable:
            dom = (
                session.query(SDTMDomain)
                .filter(SDTMDomain.standard_id == standard_id, func.lower(SDTMDomain.name) == func.lower(domain))
                .one_or_none()
            )
            if not dom: return None
            var = (
                session.query(SDTMVariable)
                .filter(SDTMVariable.domain_id == dom.id, func.lower(SDTMVariable.name) == func.lower(variable))
                .one_or_none()
            )
            return var.codelist if var and var.codelist else None
        return None

    def _normalize(self, s: str, trim=True, case_sensitive=False):
        if s is None: return ""
        s2 = s.strip() if trim else s
        return s2 if case_sensitive else s2.lower()

    def _build_synonym_index(self, session, nci_codelist_code, trim=True, case_sensitive=False):
        cl = self._latest_codelist(session, nci_codelist_code)
        if not cl:
            return None, {}
        idx = {}  # norm -> (submission_value, match_type, term_code, raw_synonym?)
        terms = (
            session.query(SDTMCodelistTerm)
            .filter(SDTMCodelistTerm.codelist_id == cl.id)
            .all()
        )
        for t in terms:
            # submission value as a key
            norm_sub = self._normalize(t.submission_value or "", trim=trim, case_sensitive=case_sensitive)
            if norm_sub:
                idx.setdefault(norm_sub, (t.submission_value, "submission", t.nci_term_code, None))
            # synonyms (split on ';')
            syns = [s.strip() for s in (t.synonyms or "").split(";") if s.strip()]
            for syn in syns:
                norm_syn = self._normalize(syn, trim=trim, case_sensitive=case_sensitive)
                if norm_syn and norm_syn not in idx:
                    idx[norm_syn] = (t.submission_value, "synonym", t.nci_term_code, syn)
        return cl, idx

    # ---------- main ----------
    def suggest(self, source_file_id: int, assign: dict, where: dict | None,
                match_options: dict | None, top_n: int, max_rows: int,
                standard_id: int | None = None,
                domain: str | None = None,
                variable: str | None = None):
        session = Session()
        try:
            # figure out columns we need
            assigns_cols = self._needed_cols_from_assign(assign)
            where_cols = self._list_where_fields(where) if where else []
            needed_cols = sorted({*assigns_cols, *[c for c in where_cols if c]})
            df = self._load_source_subset(session, source_file_id, needed_cols, max_rows)

            col_types = self._col_types_map(session, source_file_id)
            mask = self.utils.eval_where(df, where, col_types=col_types) if where else pd.Series(True, index=df.index)
            if mask.sum() == 0:
                return {"from_values": [], "suggestions": [], "codelist": None}

            df_sub = df[mask]
            base = self.utils.eval_assign_series(df_sub, assign)
            fb = assign.get("fallback")
            if fb:
                base = self.utils.apply_fallback(base, df_sub, fb)

            # collect from-values (top_n distinct)
            vc = base.value_counts(dropna=False).head(top_n)
            from_values = [{"value": (None if pd.isna(v) else v), "display": ("" if pd.isna(v) else str(v)), "count": int(n)} for v, n in vc.items()]

            # codelist resolution + index
            trim = bool(match_options.get("trim", True)) if match_options else True
            case_sensitive = bool(match_options.get("case_sensitive", False)) if match_options else False
            code = self._resolve_codelist_code(session, standard_id, domain, variable, codelist_code=None)
            if not code:
                return {
                    "from_values": from_values,
                    "suggestions": [],
                    "codelist": None,
                    "warning": f"No codelist for {domain}.{variable} in standard {standard_id}",
                }

            cl, syn_index = self._build_synonym_index(session, code, trim=trim, case_sensitive=case_sensitive)
            if not cl:
                return {"from_values": from_values, "suggestions": [], "codelist": None}

            # make suggestions
            suggestions = []
            for item in from_values:
                raw = item["value"]
                disp = item["display"]
                norm = self._normalize("" if raw is None else str(raw), trim=trim, case_sensitive=case_sensitive)
                if not norm:
                    continue
                hit = syn_index.get(norm)
                if hit:
                    to_val, match_type, term_code, raw_syn = hit
                    suggestions.append({
                        "from": raw,
                        "from_display": disp,
                        "to": to_val,
                        "match_type": match_type,      # "submission" or "synonym"
                        "term_code": term_code,
                        "synonym_matched": raw_syn,    # only when match_type == "synonym"
                    })

            return {
                "from_values": from_values,
                "suggestions": suggestions,
                "codelist": {
                    "nci_code": cl.nci_code,
                    "name": cl.name,
                    "extensible": cl.extensible,
                    "standard_name": cl.standard_name,
                    "standard_date": cl.standard_date.isoformat() if cl.standard_date else None,
                },
            }
        finally:
            session.close()
