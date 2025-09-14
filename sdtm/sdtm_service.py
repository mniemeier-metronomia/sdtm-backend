import pandas as pd
from collections import defaultdict
from db import Session, SDTMDomain, SDTMData, SDTMColumn, SDTMStandard, SDTMVariable, MappingSchema, MappingSchemaSourceFile
from sqlalchemy import and_, or_, tuple_, distinct, cast, Float
from sqlalchemy.sql import func
from source_files.source_files_utilities import SourceFilesUtilities


class SDTMService:

    def __init__(self):
        self.utilities = SourceFilesUtilities()
        

    def get_standards(self, include_domains = False):
        session = Session()
        try:
            # Aggregate per standard: domain count, variable count, and distinct classes
            q = (
                session.query(
                    SDTMStandard.id,
                    SDTMStandard.name,
                    SDTMStandard.version,
                    SDTMStandard.description,
                    func.count(distinct(SDTMDomain.id)).label("domain_count"),
                    func.count(SDTMVariable.id).label("variable_count"),
                    func.array_agg(func.distinct(SDTMDomain.sdtm_class)).label("classes"),
                )
                .outerjoin(SDTMDomain, SDTMDomain.standard_id == SDTMStandard.id)
                .outerjoin(SDTMVariable, SDTMVariable.domain_id == SDTMDomain.id)
                .group_by(SDTMStandard.id, SDTMStandard.name, SDTMStandard.version, SDTMStandard.description)
                .order_by(SDTMStandard.name.asc(), SDTMStandard.version.asc())
            )

            standards = []
            for row in q.all():
                std = {
                    "id": row.id,
                    "name": row.name,
                    "version": row.version,
                    "description": row.description,
                    "domain_count": int(row.domain_count or 0),
                    "variable_count": int(row.variable_count or 0),
                    # array_agg can include NULL; filter them out
                    "classes": [c for c in (row.classes or []) if c],
                }

                if include_domains:
                    dom_q = (
                        session.query(
                            SDTMDomain.id,
                            SDTMDomain.name,
                            SDTMDomain.label,
                            SDTMDomain.sdtm_class,
                            SDTMDomain.structure,
                            func.count(SDTMVariable.id).label("variable_count"),
                        )
                        .outerjoin(SDTMVariable, SDTMVariable.domain_id == SDTMDomain.id)
                        .filter(SDTMDomain.standard_id == row.id)
                        .group_by(
                            SDTMDomain.id,
                            SDTMDomain.name,
                            SDTMDomain.label,
                            SDTMDomain.sdtm_class,
                            SDTMDomain.structure,
                        )
                        .order_by(SDTMDomain.name.asc())
                    )
                    std["domains"] = [
                        {
                            "id": d.id,
                            "name": d.name,
                            "label": d.label,
                            "class": d.sdtm_class,
                            "structure": d.structure,
                            "variable_count": int(d.variable_count or 0),
                        }
                        for d in dom_q.all()
                    ]

                standards.append(std)

            return standards
        finally:
            session.close()


    def get_domain_variables_by_code(self, standard_id, domain_code):
        session = Session()
        try:
            dom = (
                session.query(SDTMDomain)
                .filter(
                    SDTMDomain.standard_id == standard_id,
                    func.upper(SDTMDomain.name) == func.upper(func.trim(domain_code)),
                )
                .one_or_none()
            )
            if dom is None:
                return {"domain": None, "variables": []}

            vars_q = (
                session.query(SDTMVariable)
                .filter(SDTMVariable.domain_id == dom.id)
                .order_by(
                    SDTMVariable.variable_order.asc().nullslast(),
                    SDTMVariable.name.asc(),
                )
            )

            variables = [
                {
                    "id": v.id,
                    "name": v.name,
                    "label": v.label,
                    "data_type": v.data_type,
                    "required": bool(v.required),
                    "codelist": v.codelist,
                    "role": v.role,
                    "variable_order": v.variable_order,
                    "core": v.core,
                    "described_value_domain": v.described_value_domain,
                    "value_list": v.value_list,
                    "cdisc_notes": v.cdisc_notes,
                }
                for v in vars_q.all()
            ]

            domain_payload = {
                "id": dom.id,
                "name": dom.name,
                "label": dom.label,
                "class": dom.sdtm_class,
                "structure": dom.structure,
                "standard_id": dom.standard_id,
            }

            return {"domain": domain_payload, "variables": variables}

        finally:
            session.close()


    def get_sdtm_data(
        self,
        domain,
        mapping_schema_id,              # required
        source_file_id=None,            # optional; if None, stack rows across all files
        offset=0,
        limit=100,
        sort_by=None,
        sort_dir="asc",
        filters=None,
    ):
        session = Session()
        try:
            dom = (domain or "").upper().strip()
            if not dom:
                return {"error": "domain required"}, 400
            if mapping_schema_id is None:
                return {"error": "mapping_schema_id required"}, 400

            # ---------- load headers for scope (optionally across all files) ----------
            hdr_q = (
                session.query(
                    SDTMColumn.id.label("col_id"),
                    SDTMColumn.source_file_id.label("sfid"),
                    SDTMVariable.name.label("var_name"),
                    SDTMVariable.data_type.label("ig_type"),         # "Char"/"Num"
                    SDTMVariable.variable_order.label("var_order"),
                )
                .join(SDTMVariable, SDTMVariable.id == SDTMColumn.sdtm_variable_id)
                .join(SDTMDomain, SDTMDomain.id == SDTMVariable.domain_id)
                .filter(
                    SDTMColumn.mapping_schema_id == mapping_schema_id,
                    SDTMDomain.name == dom,
                )
            )
            if source_file_id is not None:
                hdr_q = hdr_q.filter(SDTMColumn.source_file_id == source_file_id)

            hdr_rows = hdr_q.order_by(SDTMVariable.variable_order.asc(), SDTMVariable.name.asc()).all()
            if not hdr_rows:
                return {"cols": [], "rows": [], "total": 0}, 200

            # Deduplicate columns by variable name (IG), keep IG order & dtype
            var_meta_by_name = {}
            col_ids_by_var = {}     # var_name -> set(col_id across files)
            sfids = set()
            for r in hdr_rows:
                sfids.add(r.sfid)
                if r.var_name not in var_meta_by_name:
                    dt = "num" if (r.ig_type or "").lower().startswith("num") else "char"
                    var_meta_by_name[r.var_name] = {"ordinal": r.var_order, "data_type": dt}
                col_ids_by_var.setdefault(r.var_name, set()).add(r.col_id)

            # Build final ordered column list by IG order
            ordered_vars = sorted(var_meta_by_name.keys(), key=lambda n: var_meta_by_name[n]["ordinal"])
            cols = [
                {
                    "name": vn,
                    "data_type": var_meta_by_name[vn]["data_type"],
                    "nullable": True,
                    "ordinal": var_meta_by_name[vn]["ordinal"],
                }
                for vn in ordered_vars
            ]
            name_to_dtype = {vn: var_meta_by_name[vn]["data_type"] for vn in ordered_vars}

            # guard sort_by
            if sort_by and sort_by not in name_to_dtype:
                sort_by = None

            # ---------- filters: AND across variables, case-insensitive contains ----------
            filters = filters or []
            active_filters = []
            for f in filters:
                if not f:
                    continue
                vn = f.get("col")
                text = str(f.get("filter_text", "")).strip()
                if not vn or vn not in name_to_dtype or text == "":
                    continue
                active_filters.append({"var_name": vn, "text": text})

            def _escape_like(s: str) -> str:
                return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

            # Subquery of matching (sfid, row_index) if filters are present.
            # We match by variable NAME (join to SDTMVariable) so the logic works across files.
            filtered_keys_subq = None
            if active_filters:
                or_clauses = []
                for f in active_filters:
                    pattern = f"%{_escape_like(f['text'])}%"
                    or_clauses.append(and_(
                        SDTMVariable.name == f["var_name"],
                        SDTMData.value.ilike(pattern, escape="\\"),
                    ))
                filtered_keys_q = (
                    session.query(
                        SDTMColumn.source_file_id.label("sfid"),
                        SDTMData.row_index.label("ri"),
                    )
                    .join(SDTMVariable, SDTMVariable.id == SDTMColumn.sdtm_variable_id)
                    .join(SDTMDomain, SDTMDomain.id == SDTMVariable.domain_id)
                    .join(SDTMData, SDTMData.sdtm_column_id == SDTMColumn.id)
                    .filter(
                        SDTMColumn.mapping_schema_id == mapping_schema_id,
                        SDTMDomain.name == dom,
                    )
                    .filter(or_(*or_clauses))
                    .group_by(SDTMColumn.source_file_id, SDTMData.row_index)
                    .having(func.count(func.distinct(SDTMVariable.name)) == len(active_filters))
                )
                if source_file_id is not None:
                    filtered_keys_q = filtered_keys_q.filter(SDTMColumn.source_file_id == source_file_id)
                filtered_keys_subq = filtered_keys_q.subquery()

            # ---------- determine row keys (sfid, ri) with paging & optional sort ----------
            row_keys = []  # list of (sfid, ri)
            if sort_by:
                # Build a cross-file sort using values for the chosen variable name
                sort_ids = list(col_ids_by_var.get(sort_by, []))  # all column ids for this var across files
                sort_dtype = name_to_dtype.get(sort_by, "char")
                sort_expr = SDTMData.value if sort_dtype == "char" else cast(SDTMData.value, Float)
                sort_order = sort_expr.desc().nullslast() if sort_dir == "desc" else sort_expr.asc().nullsfirst()

                rk_q = (
                    session.query(
                        SDTMColumn.source_file_id.label("sfid"),
                        SDTMData.row_index.label("ri"),
                    )
                    .join(SDTMData, SDTMData.sdtm_column_id == SDTMColumn.id)
                    .filter(SDTMColumn.id.in_(sort_ids))
                )
                if filtered_keys_subq is not None:
                    rk_q = rk_q.join(
                        filtered_keys_subq,
                        and_(
                            filtered_keys_subq.c.sfid == SDTMColumn.source_file_id,
                            filtered_keys_subq.c.ri == SDTMData.row_index,
                        ),
                    )
                # Stable secondary key to keep rows grouped by file then row_index
                rk_q = rk_q.order_by(sort_order, SDTMColumn.source_file_id.asc(), SDTMData.row_index.asc())
                rk_q = rk_q.offset(offset).limit(limit)
                row_keys = [(r.sfid, r.ri) for r in rk_q.all()]
            else:
                # Default: order by file then row_index (stack vertically)
                rk_q = (
                    session.query(
                        SDTMColumn.source_file_id.label("sfid"),
                        SDTMData.row_index.label("ri"),
                    )
                    .join(SDTMData, SDTMData.sdtm_column_id == SDTMColumn.id)
                    .join(SDTMVariable, SDTMVariable.id == SDTMColumn.sdtm_variable_id)
                    .join(SDTMDomain, SDTMDomain.id == SDTMVariable.domain_id)
                    .filter(
                        SDTMColumn.mapping_schema_id == mapping_schema_id,
                        SDTMDomain.name == dom,
                    )
                    .distinct()
                )
                if source_file_id is not None:
                    rk_q = rk_q.filter(SDTMColumn.source_file_id == source_file_id)
                if filtered_keys_subq is not None:
                    rk_q = rk_q.join(
                        filtered_keys_subq,
                        and_(
                            filtered_keys_subq.c.sfid == SDTMColumn.source_file_id,
                            filtered_keys_subq.c.ri == SDTMData.row_index,
                        ),
                    )
                rk_q = rk_q.order_by(SDTMColumn.source_file_id.asc(), SDTMData.row_index.asc())
                rk_q = rk_q.offset(offset).limit(limit)
                row_keys = [(r.sfid, r.ri) for r in rk_q.all()]

            # ---------- total (distinct (sfid, ri) across scope, respecting filters) ----------
            if filtered_keys_subq is not None:
                total_count = session.query(func.count()).select_from(filtered_keys_subq).scalar()
            else:
                total_q = (
                    session.query(SDTMColumn.source_file_id, SDTMData.row_index)
                    .join(SDTMData, SDTMData.sdtm_column_id == SDTMColumn.id)
                    .join(SDTMVariable, SDTMVariable.id == SDTMColumn.sdtm_variable_id)
                    .join(SDTMDomain, SDTMDomain.id == SDTMVariable.domain_id)
                    .filter(
                        SDTMColumn.mapping_schema_id == mapping_schema_id,
                        SDTMDomain.name == dom,
                    )
                    .distinct()
                )
                if source_file_id is not None:
                    total_q = total_q.filter(SDTMColumn.source_file_id == source_file_id)
                total_count = total_q.count()

            if not row_keys:
                return {"cols": cols, "rows": [], "total": total_count}, 200

            # ---------- fetch cells for selected (sfid, ri) and shape rows ----------
            # We filter by tuple (sfid, ri)
            entries = (
                session.query(
                    SDTMColumn.source_file_id.label("sfid"),
                    SDTMData.row_index.label("ri"),
                    SDTMVariable.name.label("var_name"),
                    SDTMData.value,
                )
                .join(SDTMVariable, SDTMVariable.id == SDTMColumn.sdtm_variable_id)
                .join(SDTMData, SDTMData.sdtm_column_id == SDTMColumn.id)
                .join(SDTMDomain, SDTMDomain.id == SDTMVariable.domain_id)
                .filter(
                    SDTMColumn.mapping_schema_id == mapping_schema_id,
                    SDTMDomain.name == dom,
                )
                .filter(tuple_(SDTMColumn.source_file_id, SDTMData.row_index).in_(row_keys))
                .all()
            )

            # Build a quick lookup: (sfid, ri) -> {var_name: value}
            grouped = {}
            for sfid_val, ri_val, var_name, val in entries:
                grouped.setdefault((sfid_val, ri_val), {})[var_name] = val

            # Final rows: stack per selected keys, fill missing vars with None
            rows = []
            for key in row_keys:
                vals = grouped.get(key, {})
                rows.append({vn: vals.get(vn) for vn in ordered_vars})

            return {"cols": cols, "rows": rows, "total": total_count}, 200

        finally:
            session.close()



    def get_sdtm_overview(
        self,
        domain: str,
        source_file_id: int | None,
        mapping_schema_id: int,     # required
        stats: bool = False,
        top_k: int = 3,
    ):
        session = Session()
        try:
            dom = (domain or "").upper().strip()

            # ----- headers (IG order) across the scope -----
            hdr_q = (
                session.query(
                    SDTMColumn.id.label("col_id"),
                    SDTMColumn.source_file_id.label("sfid"),
                    SDTMVariable.name.label("var_name"),
                    SDTMVariable.data_type.label("ig_type"),        # "Char"/"Num"
                    SDTMVariable.variable_order.label("var_order"),
                    SDTMVariable.label.label("var_label"),
                )
                .join(SDTMVariable, SDTMVariable.id == SDTMColumn.sdtm_variable_id)
                .join(SDTMDomain, SDTMDomain.id == SDTMVariable.domain_id)
                .filter(
                    SDTMColumn.mapping_schema_id == mapping_schema_id,
                    SDTMDomain.name == dom,
                )
            )
            if source_file_id is not None:
                hdr_q = hdr_q.filter(SDTMColumn.source_file_id == source_file_id)

            hdr_rows = hdr_q.order_by(SDTMVariable.variable_order.asc(), SDTMVariable.name.asc()).all()
            if not hdr_rows:
                return {
                    "domain": dom,
                    "mapping_schema_id": mapping_schema_id,
                    "source_file_id": source_file_id,
                    "num_rows": 0,
                    "num_columns": 0,
                    "stats_included": False,
                    "columns": [],
                }

            # Dedup columns by IG variable name; keep order/dtype/label
            var_meta_by_name = {}
            col_ids_by_var = {}   # var_name -> set of SDTMColumn.ids (across files)
            for r in hdr_rows:
                if r.var_name not in var_meta_by_name:
                    dtype = "float" if (r.ig_type or "").lower().startswith("num") else "text"
                    var_meta_by_name[r.var_name] = {
                        "ordinal": r.var_order,
                        "data_type": dtype,
                        "label": r.var_label,
                    }
                col_ids_by_var.setdefault(r.var_name, set()).add(r.col_id)

            ordered_vars = sorted(var_meta_by_name.keys(), key=lambda n: var_meta_by_name[n]["ordinal"])
            columns = [
                {
                    "name": vn,
                    "ordinal": var_meta_by_name[vn]["ordinal"],
                    "data_type": var_meta_by_name[vn]["data_type"],
                    "description": var_meta_by_name[vn]["label"],
                }
                for vn in ordered_vars
            ]
            name_to_dtype = {vn: var_meta_by_name[vn]["data_type"] for vn in ordered_vars}

            # ----- total rows (distinct row keys) -----
            if source_file_id is not None:
                # single file → distinct row_index is fine
                col_ids_all = [cid for s in col_ids_by_var.values() for cid in s]
                total_rows = (
                    session.query(SDTMData.row_index)
                    .filter(SDTMData.sdtm_column_id.in_(col_ids_all))
                    .distinct()
                    .count()
                )
            else:
                # across files → count distinct (source_file_id, row_index)
                total_rows = session.query(
                    func.count(
                        func.distinct(
                            tuple_(SDTMColumn.source_file_id, SDTMData.row_index)
                        )
                    )
                ).select_from(
                    SDTMColumn
                ).join(
                    SDTMVariable, SDTMVariable.id == SDTMColumn.sdtm_variable_id
                ).join(
                    SDTMDomain, SDTMDomain.id == SDTMVariable.domain_id
                ).join(
                    SDTMData, SDTMData.sdtm_column_id == SDTMColumn.id
                ).filter(
                    SDTMColumn.mapping_schema_id == mapping_schema_id,
                    SDTMDomain.name == dom,
                ).scalar() or 0

            result = {
                "domain": dom,
                "mapping_schema_id": mapping_schema_id,
                "source_file_id": source_file_id,
                "num_rows": total_rows,
                "num_columns": len(columns),
                "stats_included": bool(stats),
                "columns": columns,
            }

            if not stats or total_rows == 0 or len(columns) == 0:
                return result

            # ----- stats per variable (aggregate across all files if needed) -----
            for col in result["columns"]:
                vn = col["name"]
                dtype = name_to_dtype.get(vn, "text")
                col_ids = list(col_ids_by_var.get(vn, []))
                if not col_ids:
                    continue

                # Fetch values via ORM and build a Series (simplest cross-db way)
                vals = (
                    session.query(SDTMData.value)
                    .filter(SDTMData.sdtm_column_id.in_(col_ids), SDTMData.value.isnot(None))
                    .all()
                )
                ser = pd.Series([v for (v,) in vals], dtype="object")

                col_stats = self.utilities.compute_stats(ser, total_rows, dtype, top_k)
                col.update(col_stats)

            return result

        finally:
            session.close()


    def get_mapped_domains(self, mapping_schema_id):
        """
        Aggregate mapped SDTM domains across all MappingSchemaSourceFile rows for a schema
        by parsing mapping_json.domains[*].domain.

        Returns:
        {
          "mapping_schema_id": 7,
          "standard_id": 123,
          "domains": [
            {"code": "VS", "label": "Vital Signs", "known_in_standard": true, "source_file_ids": [147, 148]},
            ...
          ]
        }
        """
        session = Session()
        try:
            ms = session.get(MappingSchema, mapping_schema_id)
            if not ms:
                return {"error": "Mapping schema not found", "status": 404}

            # Pull all link rows with mapping_json
            links = (
                session.query(MappingSchemaSourceFile.source_file_id, MappingSchemaSourceFile.mapping_json)
                .filter(MappingSchemaSourceFile.mapping_schema_id == mapping_schema_id)
                .all()
            )

            domain_to_sources = defaultdict(set)

            for sfid, mj in links:
                if not mj:
                    continue
                domains_cfg = mj.get("domains") or []
                if not isinstance(domains_cfg, list):
                    continue
                for dcfg in domains_cfg:
                    code = str((dcfg or {}).get("domain", "")).strip().upper()
                    if not code:
                        continue
                    domain_to_sources[code].add(sfid)

            codes = sorted(domain_to_sources.keys())

            # validate codes against the schema's SDTM standard and attach labels
            label_by_code = {}
            known_set = set()
            
            # validate
            dom_rows = (
                session.query(SDTMDomain.name, SDTMDomain.label)
                .filter(SDTMDomain.standard_id == ms.sdtm_standard_id,
                        SDTMDomain.name.in_(codes))
                .all()
            )
            for name, label in dom_rows:
                label_by_code[name] = label
                known_set.add(name)

            # Build response list
            domains_out = []
            for code in codes:
                entry = {
                    "code": code,
                    "label": label_by_code.get(code),              # None if unknown or not validating
                    "known_in_standard": (code in known_set),
                    "source_file_ids": sorted(domain_to_sources[code]),
                }
                domains_out.append(entry)

            return {
                "mapping_schema_id": mapping_schema_id,
                "standard_id": ms.sdtm_standard_id,
                "domains": domains_out,
            }

        finally:
            session.close()



