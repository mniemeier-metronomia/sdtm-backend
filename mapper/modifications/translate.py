import re

def _esc(s):
    return re.escape("" if s is None else str(s))

def ui_mods_to_server_ops(ui_mods):
    """
    Convert UI-layer mods (type/params) into ops for ModificationsService.apply.
    Returns (ops, ignored), preserving UI order and skipping disabled items.
    """
    ops, ignored = [], []
    for m in ui_mods or []:
        if m.get("enabled") is False:
            continue
        t = (m.get("type") or "").lower()
        p = m.get("params") or {}

        if t == "replace":
            pattern = p.get("find") or ""
            if not p.get("regex"):
                pattern = _esc(pattern)
            flags = "" if p.get("case_sensitive") else "i"
            ops.append({"op": "regex_replace", "pattern": pattern, "repl": p.get("replace", ""), "flags": flags})

        elif t == "pad":
            side = (p.get("side") or "left").lower()
            width = int(p.get("length", 0))
            fill = (str(p.get("char", "0")) or "0")[0]
            ops.append({"op": "pad_right" if side == "right" else "pad_left", "width": width, "fillchar": fill})

        elif t == "case":
            mode = (p.get("mode") or "upper").lower()
            if mode in {"upper", "lower", "title"}:
                ops.append({"op": mode})
            else:
                ignored.append(t)

        elif t == "value_map":
            if p.get("trim"):
                ops.append({"op": "trim"})
            ops.append({
                "op": "value_map",
                "map": p.get("map") or {},
                "default": p.get("default", None),
                "case_insensitive": not p.get("case_sensitive", False),
            })

        elif t == "substring_pos":
            start = int(p.get("start", 0))
            if p.get("length") is not None:
                ops.append({"op": "substr", "start": start, "length": int(p["length"])})
            else:
                ops.append({"op": "substr", "start": start})

        elif t == "to_numeric":
            thousands = p.get("thousands_sep", ",")
            decimal = p.get("decimal_sep", ".")
            if thousands:
                ops.append({"op": "regex_replace", "pattern": _esc(thousands), "repl": ""})
            if decimal and decimal != ".":
                ops.append({"op": "regex_replace", "pattern": _esc(decimal), "repl": "."})
            ops.append({"op": "to_numeric", "errors": "ignore" if p.get("coerce") is False else "coerce"})

        elif t == "datetime_parse":
            ops.append({"op": "to_datetime", "format": p.get("input_format") or None, "errors": "coerce", "utc": False})

        elif t == "trim":
            ops.append({"op": "trim"})

        elif t == "unit_convert":
            ops.append({
                "op": "unit_convert",
                "rule": p.get("rule", ""),     # "(x * 9/5) + 32"
                "round": p.get("round"),       # int or None
                "where": p.get("where"),         # rule/group object for row masking
                # 'from'/'to' are UI hints only
            })

        elif t == "format":
            ops.append({
                "op": "format",
                "fmt": p.get("fmt", "{:.1f}"),
                "na": p.get("na", None),
                "where": p.get("where"),
            })

        else:
            # e.g. substring_scan / concat (not per-column), or unknown
            ignored.append(t)

    return ops, ignored
