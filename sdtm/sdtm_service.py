import pandas as pd
from db import Session, SDTMDomain, SDTMData, SDTMColumn, SDTMStandard, SDTMVariable
from sqlalchemy import and_, or_, func, distinct, cast, Integer, Float, Boolean, DateTime
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
        source_file_id=None,
        mapping_schema_id=None,
        offset=0,
        limit=100,
        sort_by=None,
        sort_dir="asc",
        filters=None,
    ):
        session = Session()
        try:
            # ---------- columns (prefer SDTMColumn) ----------
            col_q = session.query(SDTMColumn).filter(SDTMColumn.domain == domain)
            if mapping_schema_id:
                col_q = col_q.filter(SDTMColumn.mapping_schema_id == mapping_schema_id)
            if source_file_id is not None:
                col_q = col_q.filter(SDTMColumn.source_file_id == source_file_id)
            col_q = col_q.order_by(SDTMColumn.ordinal.asc().nullsfirst(), SDTMColumn.name.asc())
            col_rows = col_q.all()

            if col_rows:
                cols = [
                    {
                        "name": c.name,
                        "data_type": (c.data_type or "text").lower(),
                        "nullable": True,
                        "ordinal": c.ordinal,
                    }
                    for c in col_rows
                ]
                col_order = [c.name for c in col_rows]
                name_to_dtype = {c.name: (c.data_type or "text").lower() for c in col_rows}
            else:
                # Fallback: infer columns from data scope
                colnames_q = session.query(SDTMData.column_name).filter(SDTMData.domain == domain)
                if mapping_schema_id:
                    colnames_q = colnames_q.filter(SDTMData.mapping_schema_id == mapping_schema_id)
                if source_file_id is not None:
                    colnames_q = colnames_q.filter(SDTMData.source_file_id == source_file_id)
                colnames = sorted({r.column_name for r in colnames_q.distinct().all()})
                cols = [{"name": n, "data_type": "text", "nullable": True, "ordinal": i} for i, n in enumerate(colnames)]
                col_order = colnames
                name_to_dtype = {n: "text" for n in colnames}

            # Guard: unknown sort_by -> disable sorting
            if sort_by and sort_by not in name_to_dtype:
                sort_by = None

            # ---------- normalize filters (AND across columns, case-insensitive contains) ----------
            filters = filters or []
            active_filters = []
            for f in filters:
                if not f:
                    continue
                col = f.get("col")
                text = str(f.get("filter_text", "")).strip()
                if not col or col not in name_to_dtype or text == "":
                    continue
                active_filters.append({"col": col, "text": text})

            def _escape_like(s: str) -> str:
                # escape %, _ and \ for LIKE/ILIKE
                return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

            # Build filtered row subquery (rows must match ALL filters)
            # ((col=a AND value ILIKE %x%) OR (col=b AND value ILIKE %y%) ...)
            # GROUP BY row_index HAVING COUNT(DISTINCT column_name) = N_filters
            filtered_rows_subq = None
            if active_filters:
                base_scope = session.query(SDTMData).filter(SDTMData.domain == domain)
                if mapping_schema_id:
                    base_scope = base_scope.filter(SDTMData.mapping_schema_id == mapping_schema_id)
                if source_file_id is not None:
                    base_scope = base_scope.filter(SDTMData.source_file_id == source_file_id)

                or_clauses = []
                for f in active_filters:
                    pattern = f"%{_escape_like(f['text'])}%"
                    or_clauses.append(and_(SDTMData.column_name == f["col"], SDTMData.value.ilike(pattern, escape="\\")))

                filtered_rows_q = (
                    base_scope.filter(or_(*or_clauses))
                    .with_entities(SDTMData.row_index)
                    .group_by(SDTMData.row_index)
                    .having(func.count(func.distinct(SDTMData.column_name)) == len(active_filters))
                )
                filtered_rows_subq = filtered_rows_q.subquery()

            # ---------- row indices (respect filters; optional sort) ----------
            if sort_by:
                dtype = name_to_dtype.get(sort_by, "text")
                sort_expr = SDTMData.value
                if dtype in ("int", "integer"):
                    sort_expr = cast(SDTMData.value, Integer)
                elif dtype in ("float", "decimal", "double", "numeric"):
                    sort_expr = cast(SDTMData.value, Float)
                elif dtype in ("datetime", "timestamp", "date"):
                    sort_expr = cast(SDTMData.value, DateTime)
                elif dtype in ("bool", "boolean"):
                    sort_expr = cast(SDTMData.value, Boolean)

                sort_order = sort_expr.desc().nullslast() if sort_dir == "desc" else sort_expr.asc().nullsfirst()

                ri_q = session.query(SDTMData.row_index).filter(SDTMData.domain == domain, SDTMData.column_name == sort_by)
                if mapping_schema_id:
                    ri_q = ri_q.filter(SDTMData.mapping_schema_id == mapping_schema_id)
                if source_file_id is not None:
                    ri_q = ri_q.filter(SDTMData.source_file_id == source_file_id)
                if filtered_rows_subq is not None:
                    ri_q = ri_q.filter(SDTMData.row_index.in_(filtered_rows_subq))

                row_indices = [
                    r.row_index
                    for r in ri_q.order_by(sort_order, SDTMData.row_index.asc()).offset(offset).limit(limit).all()
                ]
            else:
                ri_q = session.query(SDTMData.row_index).filter(SDTMData.domain == domain)
                if mapping_schema_id:
                    ri_q = ri_q.filter(SDTMData.mapping_schema_id == mapping_schema_id)
                if source_file_id is not None:
                    ri_q = ri_q.filter(SDTMData.source_file_id == source_file_id)
                if filtered_rows_subq is not None:
                    ri_q = ri_q.filter(SDTMData.row_index.in_(filtered_rows_subq))

                row_indices = [
                    r.row_index
                    for r in ri_q.distinct().order_by(SDTMData.row_index.asc()).offset(offset).limit(limit).all()
                ]

            # ---------- total (respect filters) ----------
            if filtered_rows_subq is not None:
                total_count = session.query(func.count()).select_from(filtered_rows_subq).scalar()
            else:
                total_q = session.query(SDTMData.row_index).filter(SDTMData.domain == domain)
                if mapping_schema_id:
                    total_q = total_q.filter(SDTMData.mapping_schema_id == mapping_schema_id)
                if source_file_id is not None:
                    total_q = total_q.filter(SDTMData.source_file_id == source_file_id)
                total_count = total_q.distinct().count()

            if not row_indices:
                return {"cols": cols, "rows": [], "total": total_count}, 200

            # ---------- fetch cells ----------
            data_q = session.query(SDTMData).filter(SDTMData.domain == domain, SDTMData.row_index.in_(row_indices))
            if mapping_schema_id:
                data_q = data_q.filter(SDTMData.mapping_schema_id == mapping_schema_id)
            if source_file_id is not None:
                data_q = data_q.filter(SDTMData.source_file_id == source_file_id)
            entries = data_q.all()

            grouped = {}
            for e in entries:
                grouped.setdefault(e.row_index, {})[e.column_name] = e.value

            rows = []
            for idx in row_indices:
                rec = grouped.get(idx, {})
                rows.append({k: rec.get(k) for k in col_order})

            return {"cols": cols, "rows": rows, "total": total_count}, 200

        finally:
            session.close()


    def get_sdtm_overview(self, domain: str, source_file_id: int | None, mapping_schema_id: int | None,
                           stats: bool = False, top_k: int = 3):
        session = Session()
        try:
            # ---------- columns (prefer SDTMColumn) ----------
            col_q = session.query(SDTMColumn).filter(SDTMColumn.domain == domain)
            if mapping_schema_id:
                col_q = col_q.filter(SDTMColumn.mapping_schema_id == mapping_schema_id)
            if source_file_id is not None:
                col_q = col_q.filter(SDTMColumn.source_file_id == source_file_id)
            col_q = col_q.order_by(SDTMColumn.ordinal.asc().nullsfirst(), SDTMColumn.name.asc())
            col_rows = col_q.all()

            if col_rows:
                columns = [
                    {
                        "name": c.name,
                        "ordinal": c.ordinal,
                        "data_type": (c.data_type or "text").lower(),
                        "description": None,  # (optional) could be filled from SDTMVariable.label if you pass standard_id
                    }
                    for c in col_rows
                ]
                name_to_dtype = {c.name: (c.data_type or "text").lower() for c in col_rows}
            else:
                # Fallback to distinct column names in SDTMData for this scope
                names_q = session.query(SDTMData.column_name).filter(SDTMData.domain == domain)
                if mapping_schema_id:
                    names_q = names_q.filter(SDTMData.mapping_schema_id == mapping_schema_id)
                if source_file_id is not None:
                    names_q = names_q.filter(SDTMData.source_file_id == source_file_id)
                names = sorted({r.column_name for r in names_q.distinct().all()})
                columns = [
                    {"name": n, "ordinal": i, "data_type": "text", "description": None}
                    for i, n in enumerate(names)
                ]
                name_to_dtype = {n: "text" for n in names}

            # ---------- row count (distinct row_index for scope) ----------
            total_q = session.query(SDTMData.row_index).filter(SDTMData.domain == domain)
            if mapping_schema_id:
                total_q = total_q.filter(SDTMData.mapping_schema_id == mapping_schema_id)
            if source_file_id is not None:
                total_q = total_q.filter(SDTMData.source_file_id == source_file_id)
            total_rows = total_q.distinct().count()

            result = {
                "domain": domain,
                "mapping_schema_id": mapping_schema_id,
                "source_file_id": source_file_id,
                "num_rows": total_rows,
                "num_columns": len(columns),
                "stats_included": bool(stats),
                "columns": columns,  # stats will be flattened into each dict below
            }

            if not stats or total_rows == 0 or len(columns) == 0:
                return result

            # ---------- compute stats per column (pandas) ----------
            engine = session.get_bind()
            for col in result["columns"]:
                name = col["name"]
                dtype = col.get("data_type") or name_to_dtype.get(name, "text")

                df_col = pd.read_sql_query(
                    """
                    SELECT value
                    FROM sdtm_data
                    WHERE domain = %(domain)s
                      AND column_name = %(col)s
                      AND value IS NOT NULL
                      {schema_filter}
                      {file_filter}
                    """.format(
                        schema_filter="AND mapping_schema_id = %(msid)s" if mapping_schema_id else "",
                        file_filter="AND source_file_id = %(sfid)s" if source_file_id is not None else "",
                    ),
                    con=engine,
                    params={
                        "domain": domain,
                        "col": name,
                        **({"msid": mapping_schema_id} if mapping_schema_id else {}),
                        **({"sfid": source_file_id} if source_file_id is not None else {}),
                    },
                )

                ser = df_col["value"] if "value" in df_col.columns else pd.Series([], dtype="object")
                # Reuse your raw compute_stats (flat dict) for identical math
                col_stats = self.utilities.compute_stats(ser, total_rows, dtype, top_k)
                col.update(col_stats)  # flatten: nulls/distinct/top/... on the column dict

            return result

        finally:
            session.close()

