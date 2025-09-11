import pandas as pd
import re


class ModificationsService:
    """
    Apply a linear sequence of simple transforms to a Series.
    No registry, just if/elif ops for clarity & control.
    """
    def apply(self, s, transforms):
        if not transforms:
            return s
        out = s.copy()
        for t in transforms:
            op = (t.get("op") or "").lower()

            if op == "trim":
                out = out.astype("string").str.strip()

            elif op == "lower":
                out = out.astype("string").str.lower()

            elif op == "upper":
                out = out.astype("string").str.upper()

            elif op == "title":
                out = out.astype("string").str.title()

            elif op == "regex_replace":
                # {"op":"regex_replace","pattern":"\\s*(cm|kg)$","repl":"" ,"flags":"i"}
                pat = t.get("pattern", "")
                repl = t.get("repl", "")
                flags = 0
                if "i" in (t.get("flags","").lower()):
                    flags |= re.IGNORECASE
                out = out.astype("string").str.replace(pat, repl, regex=True, flags=flags)

            elif op == "value_map":
                # {"op":"value_map","map":{"M":"Male","F":"Female"},"default":null,"case_insensitive":true}
                m = (t.get("map") or {})
                default = t.get("default", None)
                ci = bool(t.get("case_insensitive", False))
                if ci:
                    m_ci = { (k.lower() if isinstance(k,str) else k): v for k,v in m.items() }
                    def _map(v):
                        if isinstance(v,str): return m_ci.get(v.lower(), default if default is not None else v)
                        return m_ci.get(v, default if default is not None else v)
                    out = out.map(_map)
                else:
                    out = out.map(lambda v: m.get(v, default if default is not None else v))

            elif op == "to_numeric":
                # {"op":"to_numeric","errors":"coerce"}
                out = pd.to_numeric(out, errors=t.get("errors","coerce"))

            elif op == "to_datetime":
                # {"op":"to_datetime","format":"%d-%m-%Y","errors":"coerce","utc":false}
                out = pd.to_datetime(out, format=t.get("format"), errors=t.get("errors","coerce"), utc=bool(t.get("utc", False)))

            elif op == "fillna":
                out = out.fillna(t.get("value"))

            elif op == "clip":
                out = pd.to_numeric(out, errors="coerce").clip(lower=t.get("lower"), upper=t.get("upper"))

            elif op == "round":
                out = pd.to_numeric(out, errors="coerce").round(int(t.get("decimals", 0)))

            elif op == "units_strip":
                # {"op":"units_strip","pattern":"\\s*(cm|mmHg|kg)$"}
                pat = t.get("pattern", r"\s*(cm|mmHg|kg|mg|g|lbs|lb)$")
                out = out.astype("string").str.replace(pat, "", regex=True).str.strip()

            elif op == "pad_left":
                # {"op":"pad_left","width":3,"fillchar":"0"}
                width = int(t.get("width", 0))
                fillchar = str(t.get("fillchar","0"))[:1]
                out = out.astype("string").str.pad(width, side="left", fillchar=fillchar)

            elif op == "substr":
                # {"op":"substr","start":0,"length":5}  (length optional)
                start = int(t.get("start", 0))
                length = t.get("length", None)
                s_str = out.astype("string").str
                out = s_str.slice(start, start + int(length)) if length is not None else s_str.slice(start)

            elif op == "pad_right":
                # {"op":"pad_right","width":3,"fillchar":"0"}
                width = int(t.get("width", 0))
                fillchar = str(t.get("fillchar","0"))[:1]
                out = out.astype("string").str.pad(width, side="right", fillchar=fillchar)

            # else: unknown op -> ignore silently (or raise if you prefer)
        return out
