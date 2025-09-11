# services/mapper_service.py
import re
import pandas as pd


class TransformerUtils:

    def _resolve_col(self, df: pd.DataFrame, name: str | None) -> str | None:
        if not name:
            return None
        if name in df.columns:
            return name
        # case-insensitive lookup
        lower_map = getattr(self, "_lower_map_cache", None)
        if lower_map is None or lower_map.get("__df_id__") is not id(df):
            lower_map = {c.lower(): c for c in df.columns}
            lower_map["__df_id__"] = id(df)
            self._lower_map_cache = lower_map
        return lower_map.get(str(name).lower())
    

    def _canon_base(self, dt: str | None):
        d = (dt or "").lower().strip()
        if d.endswith("?"):
            d = d[:-1]
        mapping = {
            "int64": "int", "int32": "int", "integer": "int", "int": "int",
            "float64": "float", "float32": "float", "float": "float", "double": "float", "decimal": "float",
            "bool": "bool", "boolean": "bool",
            "datetime64[ns]": "datetime", "datetime": "datetime", "timestamp": "datetime", "date": "datetime",
            "object": "string", "text": "string", "string": "string"
        }
        return mapping.get(d, d or "string")

    def _coerce_value(self, val, base):
        if base in {"int", "float"}:
            try:
                return float(val)
            except Exception:
                return None
        if base == "datetime":
            try:
                v = pd.to_datetime(val, errors="coerce", utc=False)
                return v
            except Exception:
                return None
        if base == "bool":
            s = str(val).strip().lower()
            if s in {"true","1","yes","y"}: return True
            if s in {"false","0","no","n"}: return False
            return None
        # string
        return None if val is None else str(val)

    def _coerce_series(self, s, base):
        if base in {"int", "float"}:
            return pd.to_numeric(s, errors="coerce")
        if base == "datetime":
            return pd.to_datetime(s, errors="coerce", utc=False)
        if base == "bool":
            ss = s.astype("string").str.strip().str.lower()
            mapping = {"true": True, "1": True, "yes": True, "y": True,
                       "false": False, "0": False, "no": False, "n": False}
            return ss.map(mapping).astype("boolean")
        return s.astype("string")
    

    def merge_assigns_no_override(self, common_assign, emitter_assign):
        """Combine common + emitter assigns. If emitter tries to use a 'to' already in common, ignore it."""
        seen = set()
        out = []
        for a in common_assign or []:
            to = (a.get("to") or "").upper()
            if to and to not in seen:
                out.append(a); seen.add(to)
        for a in emitter_assign or []:
            to = (a.get("to") or "").upper()
            if to and to not in seen:
                out.append(a); seen.add(to)
        return out

    def eval_where(self, df, where, col_types: dict | None = None):
        """Evaluate nested AND/OR where-tree to a boolean mask over df.index, using type info when provided."""
        if where is None or (isinstance(where, dict) and len(where) == 0) or not isinstance(where, dict):
            return pd.Series(True, index=df.index)

        t = where.get("type")
        if t == "group":
            logic = (where.get("logic") or "AND").upper()
            children = where.get("children") or []
            masks = [self.eval_where(df, c, col_types=col_types) for c in children]
            if not masks:
                return pd.Series(True, index=df.index)
            out = masks[0].copy()
            for m in masks[1:]:
                out = (out | m) if logic == "OR" else (out & m)
            return out

        # rule
        field = where.get("field")
        op = (where.get("op") or "==").lower()
        val = where.get("value")

        col = self._resolve_col(df, field)
        if not col:
            return pd.Series(False, index=df.index)

        # Determine base type from metadata (if available)
        base = None
        if col_types:
            # try exact then case-insensitive
            base = col_types.get(col) or col_types.get(str(col).lower()) or col_types.get(str(field).lower())
            base = self._canon_base(base)

        s_raw = df[col]

        # Null/blank checks ignore type
        if op == "not_null":
            return s_raw.notna() & (s_raw.astype(str) != "")
        if op == "is_null":
            return s_raw.isna() | (s_raw.astype(str) == "")

        # String operators stay string-based
        if op == "contains":
            return s_raw.astype(str).str.contains(str(val), na=False)
        if op in ("starts_with", "startswith"):
            return s_raw.astype(str).str.startswith(str(val), na=False)
        if op in ("ends_with", "endswith"):
            return s_raw.astype(str).str.endswith(str(val), na=False)
        if op == "regex":
            return s_raw.astype(str).str.contains(str(val), regex=True, na=False)

        # For typed ops, coerce Series and values first (fallback to previous behavior if no base)
        base = base or "string"
        s = self._coerce_series(s_raw, base)

        # Equality / inequality
        if op in ("==", "eq", "!=", "ne"):
            if base in {"int","float","datetime","bool"}:
                v = self._coerce_value(val, base)
                cmp = (s == v)
            else:
                cmp = s.astype(str).str.strip().eq(str(val).strip())
            return ~cmp if op in ("!=", "ne") else cmp

        # IN list
        if op == "in":
            items_raw = [x.strip() for x in str(val).split(",")]
            if base in {"int","float","datetime","bool"}:
                vals = [self._coerce_value(x, base) for x in items_raw]
                return s.isin(set(v for v in vals if v is not None))
            else:
                return s.astype(str).isin(set(items_raw))

        # Numeric/date comparisons
        if base in {"int","float","datetime"}:
            try:
                v = self._coerce_value(val, base)
            except Exception:
                v = None
            if v is None:
                return pd.Series(False, index=s.index)
            if op == ">":  return s > v
            if op == "<":  return s < v
            if op in (">=", "ge"): return s >= v
            if op in ("<=", "le"): return s <= v

        # Fallback (unknown op): don't exclude
        return pd.Series(True, index=df.index)

    _brace_re = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")

    def eval_assign_series(self, df_sub, assign):
        """Return a Series for the assign over df_sub.index."""
        mode = (assign.get("mode") or "static").lower()
        val = assign.get("value")

        if mode == "column":
            col = str(val) if val is not None else None
            if not col or col not in df_sub.columns:
                return pd.Series([None] * len(df_sub), index=df_sub.index)
            return df_sub[col]

        if mode == "static":
            return pd.Series([val] * len(df_sub), index=df_sub.index)

        if mode == "expression":
            expr = val or ""
            # If it uses {COL} tokens, treat as string template
            if "{" in expr and "}" in expr:
                return self._eval_template(df_sub, expr)

            # Otherwise try numeric/string expression via df.eval
            local_df = df_sub.copy()
            for c in local_df.columns:
                # Coerce numerics where possible; leave strings alone
                local_df[c] = pd.to_numeric(local_df[c], errors="ignore")
            try:
                result = local_df.eval(expr, engine="python")
            except Exception:
                return pd.Series([None] * len(df_sub), index=df_sub.index)
            if not isinstance(result, pd.Series):
                result = pd.Series([result] * len(df_sub), index=df_sub.index)
            result.index = df_sub.index
            return result

        # unknown mode
        return pd.Series([None] * len(df_sub), index=df_sub.index)

    def _eval_template(self, df_sub, template: str) -> pd.Series:
        pieces = []
        last = 0
        for m in self._brace_re.finditer(template):
            if m.start() > last:
                txt = template[last:m.start()]
                pieces.append(pd.Series([txt] * len(df_sub), index=df_sub.index))
            col = m.group(1)
            s = df_sub.get(col)
            if s is None:
                s = pd.Series([None] * len(df_sub), index=df_sub.index)
            # turn NaN/None into empty string for concatenation
            pieces.append(s.astype(str).where(s.notna(), ""))
            last = m.end()
        if last < len(template):
            txt = template[last:]
            pieces.append(pd.Series([txt] * len(df_sub), index=df_sub.index))

        if not pieces:
            return pd.Series([None] * len(df_sub), index=df_sub.index)

        out = pieces[0].astype(str)
        for p in pieces[1:]:
            out = out + p.astype(str)
        out.index = df_sub.index
        return out

    def apply_fallback(self, series, df_sub, fb):
        """Fill NA/empty with fallback value computed with the same rules."""
        mask_na = series.isna() | (series.astype(str) == "")
        if not mask_na.any():
            return series
        fb_mode = (fb.get("mode") or "static").lower()
        fb_val = fb.get("value")
        if fb_mode == "column":
            col = str(fb_val) if fb_val is not None else None
            fb_series = df_sub.get(col, pd.Series([None] * len(df_sub), index=df_sub.index))
        elif fb_mode == "expression":
            fb_series = self.eval_assign_series(df_sub, {"mode": "expression", "value": fb_val})
        else:
            fb_series = pd.Series([fb_val] * len(df_sub), index=df_sub.index)
        return series.mask(mask_na, fb_series)
