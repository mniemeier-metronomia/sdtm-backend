import pandas as pd
import re

class ModificationsService:
    """
    Apply a linear sequence of simple transforms to a Series.
    Supports per-op conditionality via `where` (mask over df),
    and a simple `unit_convert` with expression `rule` using x.
    """

    def apply(self, s, transforms, df=None, utils=None):
        """
        s: pandas Series (indexed to the DataFrame you'll mask against)
        transforms: list of ops like {"op":"trim"} or {"op":"unit_convert","rule":"(x-32)*5/9","where":{...}}
        df: optional pandas DataFrame providing row context for `where`
        utils: optional TransformerUtils (for eval_where)
        """
        if not transforms:
            return s

        out = s.copy()

        def _mask_for(op):
            if df is not None and op.get("where") and utils is not None:
                m = utils.eval_where(df, op["where"])
                return m.reindex(out.index, fill_value=False)
            # apply everywhere
            return pd.Series(True, index=out.index)

        def _apply_masked(series, mask, func):
            if not isinstance(mask, pd.Series):
                # assume truthy -> apply everywhere
                mask = pd.Series(True, index=series.index)
            if not mask.any():
                return series
            # transform only the masked slice
            sub = series.loc[mask]
            res = func(sub)
            series = series.copy()
            series.loc[mask] = res
            return series

        for t in transforms:
            op = (t.get("op") or "").lower()
            mask = _mask_for(t)

            if op == "trim":
                out = _apply_masked(out, mask, lambda s2: s2.astype("string").str.strip())

            elif op == "lower":
                out = _apply_masked(out, mask, lambda s2: s2.astype("string").str.lower())

            elif op == "upper":
                out = _apply_masked(out, mask, lambda s2: s2.astype("string").str.upper())

            elif op == "title":
                out = _apply_masked(out, mask, lambda s2: s2.astype("string").str.title())

            elif op == "regex_replace":
                # {"op":"regex_replace","pattern":"\\s*(cm|kg)$","repl":"","flags":"i"}
                pat = t.get("pattern", "")
                repl = t.get("repl", "")
                flags = 0
                if "i" in (t.get("flags", "").lower()):
                    flags |= re.IGNORECASE
                out = _apply_masked(out, mask, lambda s2: s2.astype("string").str.replace(pat, repl, regex=True, flags=flags))

            elif op == "value_map":
                # {"op":"value_map","map":{"M":"Male"},"default":null,"case_insensitive":true}
                m = (t.get("map") or {})
                default = t.get("default", None)
                ci = bool(t.get("case_insensitive", False))
                if ci:
                    m_ci = {(k.lower() if isinstance(k, str) else k): v for k, v in m.items()}
                    def _map_ci(v):
                        if isinstance(v, str):
                            return m_ci.get(v.lower(), default if default is not None else v)
                        return m_ci.get(v, default if default is not None else v)
                    out = _apply_masked(out, mask, lambda s2: s2.map(_map_ci))
                else:
                    out = _apply_masked(out, mask, lambda s2: s2.map(lambda v: m.get(v, default if default is not None else v)))

            elif op == "to_numeric":
                # {"op":"to_numeric","errors":"coerce"|"ignore"}
                errs = t.get("errors", "coerce")
                out = _apply_masked(out, mask, lambda s2: pd.to_numeric(s2, errors=errs))

            elif op == "to_datetime":
                # {"op":"to_datetime","format":"%d-%m-%Y","errors":"coerce","utc":false}
                fmt = t.get("format")
                errs = t.get("errors", "coerce")
                utc = bool(t.get("utc", False))
                out = _apply_masked(out, mask, lambda s2: pd.to_datetime(s2, format=fmt, errors=errs, utc=utc))

            elif op == "fillna":
                val = t.get("value")
                out = _apply_masked(out, mask, lambda s2: s2.fillna(val))

            elif op == "clip":
                low = t.get("lower", None)
                high = t.get("upper", None)
                out = _apply_masked(out, mask, lambda s2: pd.to_numeric(s2, errors="coerce").clip(lower=low, upper=high))

            elif op == "round":
                dec = int(t.get("decimals", 0))
                out = _apply_masked(out, mask, lambda s2: pd.to_numeric(s2, errors="coerce").round(dec))

            elif op == "units_strip":
                # {"op":"units_strip","pattern":"\\s*(cm|mmHg|kg)$"}
                pat = t.get("pattern", r"\s*(cm|mmHg|kg|mg|g|lbs|lb)$")
                out = _apply_masked(out, mask, lambda s2: s2.astype("string").str.replace(pat, "", regex=True).str.strip())

            elif op == "pad_left":
                width = int(t.get("width", 0))
                fillchar = str(t.get("fillchar", "0"))[:1]
                out = _apply_masked(out, mask, lambda s2: s2.astype("string").str.pad(width, side="left", fillchar=fillchar))

            elif op == "pad_right":
                width = int(t.get("width", 0))
                fillchar = str(t.get("fillchar", "0"))[:1]
                out = _apply_masked(out, mask, lambda s2: s2.astype("string").str.pad(width, side="right", fillchar=fillchar))

            elif op == "substr":
                # {"op":"substr","start":0,"length":5}
                start = int(t.get("start", 0))
                length = t.get("length", None)
                if length is not None:
                    out = _apply_masked(out, mask, lambda s2: s2.astype("string").str.slice(start, start + int(length)))
                else:
                    out = _apply_masked(out, mask, lambda s2: s2.astype("string").str.slice(start))

            elif op == "unit_convert":
                # {"op":"unit_convert","rule":"(x - 32) * 5/9","round":1|null,"where":{...}}
                expr = t.get("rule") or ""
                if not expr:
                    continue
                rnd = t.get("round", None)

                def _do_convert(s2):
                    base = pd.to_numeric(s2, errors="coerce")
                    # safe eval: only 'x' in locals, no builtins
                    out_vals = eval(expr, {"__builtins__": {}}, {"x": base})
                    if rnd is not None:
                        out_nums = pd.to_numeric(out_vals, errors="coerce").round(int(rnd))
                        return out_nums
                    return out_vals

                out = _apply_masked(out, mask, _do_convert)

            elif op == "format":
                fmt = t.get("fmt", "{}")
                na_val = t.get("na", None)

                def _do_format(s2):
                    # datetime -> strftime if fmt looks like a strftime pattern
                    if pd.api.types.is_datetime64_any_dtype(s2):
                        res = s2.dt.strftime(fmt)
                    else:
                        # try numeric formatting
                        s_num = pd.to_numeric(s2, errors="ignore")
                        if pd.api.types.is_numeric_dtype(s_num):
                            if "{" in fmt and "}" in fmt:
                                res = s_num.apply(lambda v: None if pd.isna(v) else fmt.format(v))
                            else:
                                # support plain format spec like ".1f"
                                res = s_num.apply(lambda v: None if pd.isna(v) else f"{v:{fmt}}")
                        else:
                            # generic string template; {x} -> value
                            if "{" in fmt and "}" in fmt:
                                res = s2.apply(lambda v: None if pd.isna(v) else fmt.format(v))
                            else:
                                res = s2.astype("string")
                    if na_val is not None:
                        res = res.where(res.notna(), na_val)
                    return res

                out = _apply_masked(out, mask, _do_format)


            # else: unknown op -> ignore silently

        return out
