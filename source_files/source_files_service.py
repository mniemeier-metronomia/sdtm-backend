import pandas as pd
from db import Session, SourceFile, SourceColumn, SourceData
from sqlalchemy import and_, or_, func, cast, Integer, Float, DateTime, Boolean

from source_files.source_files_utilities import SourceFilesUtilities


class SourceFilesService:

    def __init__(self):
        self.utilities = SourceFilesUtilities()


    def get_source_file_return_dict(self, sf):
        return {
            "id": sf.id,
            "project_id": sf.project_id,
            "name": sf.name,
            "num_rows": sf.num_rows,
            "num_columns": sf.num_columns,
            "key_columns": sf.key_columns or [],
            "included_columns": sf.included_columns or [],
            "table_created_at": sf.table_created_at,
            "uploaded_at": sf.uploaded_at,
        }


    def get_overview(self, source_file_id: int, stats: bool = False, top_k: int = 3):
        session = Session()
        try:
            sf = session.get(SourceFile, source_file_id)
            if not sf:
                raise ValueError("Source file not found.")

            # ---- base payload + ordered column list
            col_rows = (
                session.query(SourceColumn)
                .filter_by(source_file_id=source_file_id)
                .order_by(SourceColumn.ordinal.asc())
                .all()
            )

            result = self.get_source_file_return_dict(sf)
            result["columns"] = [
                {"name": c.name, "ordinal": c.ordinal, "data_type": c.data_type, "description": c.description}
                for c in col_rows
            ]
            if not stats:
                return result

            # maps for quick lookup
            name_to_id = {c.name: c.id for c in col_rows}
            name_to_dtype = {c.name: c.data_type for c in col_rows}

            # ---- do we have materialized source_data?
            has_source_data = (
                session.query(SourceData.id)
                .filter_by(source_file_id=source_file_id)
                .limit(1)
                .first()
                is not None
            )

            if has_source_data:
                # Estimate total rows; if unknown, compute from row_index
                total_rows = sf.num_rows or (
                    session.execute(
                        "SELECT COALESCE(MAX(row_index)+1, 0) FROM source_data WHERE source_file_id = :sfid",
                        {"sfid": sf.id},
                    ).scalar() or 0
                )

                engine = session.get_bind()

                # Compute stats per column using source_column_id (no column_name anymore)
                for col in result["columns"]:
                    col_name = col["name"]
                    col_id = name_to_id[col_name]

                    df_col = pd.read_sql_query(
                        """
                        SELECT sd.value
                        FROM source_data sd
                        WHERE sd.source_file_id = %(sfid)s
                        AND sd.source_column_id = %(colid)s
                        AND sd.value IS NOT NULL
                        """,
                        con=engine,
                        params={"sfid": sf.id, "colid": col_id},
                    )

                    ser = df_col["value"] if "value" in df_col.columns else pd.Series([], dtype="object")
                    dtype = name_to_dtype.get(col_name)
                    col_stats = self.utilities.compute_stats(ser, total_rows, dtype, top_k)
                    col.update(col_stats)  # flatten into the column dict

                return result

            # ---- Fallback: read the original large object into pandas
            raw_conn = session.connection().connection
            with raw_conn.cursor() as _:
                lo = raw_conn.lobject(sf.file_oid, mode="rb")
                content = lo.read()
                lo.close()

            df, col_desc = self.utilities.load_dataframe(content, sf.content_type, sf.name)
            result["num_rows"] = len(df)
            result["num_columns"] = len(df.columns)

            # Stats for known columns
            known = {c["name"]: c for c in result["columns"]}
            for name, meta in known.items():
                if name in df.columns:
                    col_stats = self.utilities.compute_stats(df[name], len(df), meta.get("data_type"), top_k)
                    meta.update(col_stats)
                    if meta.get("description") is None:
                        meta["description"] = col_desc.get(name)

            # Add any file-only columns not yet in SourceColumn
            for name in map(str, df.columns):
                if name not in known:
                    dtype = self.utilities.infer_data_type(df[name])
                    col_stats = self.utilities.compute_stats(df[name], len(df), dtype, top_k)
                    result["columns"].append({
                        "name": name,
                        "ordinal": len(result["columns"]),
                        "data_type": dtype,
                        "description": col_desc.get(name),
                        **col_stats,
                    })

            return result

        finally:
            session.close()


       

    def get_project_overview(self, project_id):
        session = Session()
        try:
            sfs = (
                session.query(SourceFile)
                .filter(SourceFile.project_id == project_id)
                .order_by(SourceFile.uploaded_at.desc(), SourceFile.id.desc())
                .all()
            )

            return {
                "project_id": project_id,
                "count": len(sfs),
                "source_files": [self.get_source_file_return_dict(sf) for sf in sfs],
            }
        finally:
            session.close()


    def check_key_uniqueness(self, sourcefile_id, key_columns):
        if not key_columns:
            raise ValueError("No key columns provided.")

        session = Session()
        try:
            file_record = session.query(SourceFile).get(sourcefile_id)
            if not file_record or not file_record.file_oid:
                raise ValueError("Source file not found or file OID missing.")

            connection = session.connection()
            raw_conn = connection.connection
            with raw_conn.cursor() as cursor:
                lo = raw_conn.lobject(file_record.file_oid, mode='rb')
                content = lo.read()
                lo.close()

            df, _ = self.utilities.load_dataframe(content, file_record.content_type, file_record.name)

            # Check for key uniqueness
            duplicated = df.duplicated(subset=key_columns, keep=False)
            duplicate_count = duplicated.sum()

            return {
                "is_unique": bool(duplicate_count == 0),
                "duplicate_count": int(duplicate_count)
            }


        finally:
            session.close()


    def update_source_file(self, source_file_id, key_columns = None, included_columns = None):
        session = Session()
        try:
            sf = session.get(SourceFile, source_file_id)
            if not sf:
                raise ValueError("Source file not found.")

            changed = False

            if key_columns is not None:
                if not isinstance(key_columns, (list, tuple)):
                    raise ValueError("key_columns must be a list (or null).")
                sf.key_columns = None if key_columns is None else [str(x) for x in key_columns]
                changed = True

            if included_columns is not None:
                if not isinstance(included_columns, (list, tuple)):
                    raise ValueError("included_columns must be a list (or null).")
                sf.included_columns = None if included_columns is None else [str(x) for x in included_columns]
                changed = True

            if changed:
                session.commit()

            return self.get_source_file_return_dict(sf)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


    def generate_source_data(self, source_file_id):
        session = Session()
        try:
            sf = session.get(SourceFile, source_file_id)
            if not sf:
                raise ValueError("Source file not found")

            # wipe previous (SourceData first to satisfy FK)
            session.query(SourceData).filter_by(source_file_id=source_file_id).delete()
            session.query(SourceColumn).filter_by(source_file_id=source_file_id).delete()

            # read large object -> DataFrame
            raw_conn = session.connection().connection
            with raw_conn.cursor() as cur:
                lo = raw_conn.lobject(sf.file_oid, mode="rb")
                content = lo.read()
                lo.close()

            df, col_descriptions = self.utilities.load_dataframe(
                content, sf.content_type, sf.name
            )

            raw_columns = sf.included_columns or list(df.columns)
            # keep only columns that actually exist, preserve order
            raw_columns = [c for c in raw_columns if c in df.columns]

            # ---- normalize dtypes (avoid 1.0 for ints) ----
            df = df.convert_dtypes()
            for col in raw_columns:
                s = df[col]
                if pd.api.types.is_float_dtype(s):
                    s_nonnull = s.dropna().astype(float)
                    if len(s_nonnull) and (s_nonnull % 1 == 0).all():
                        df[col] = pd.to_numeric(s, errors="coerce").astype("Int64")

            # ---- create SourceColumn + serializers ----
            serializers = {}
            column_objs = []
            for i, col in enumerate(raw_columns):
                series = df[col]
                dtype = self.utilities.infer_data_type(series)  # "int","int?","float","datetime","bool","string"
                column_objs.append(SourceColumn(
                    source_file_id=sf.id,
                    name=col,
                    data_type=dtype,
                    ordinal=i,
                    description=col_descriptions.get(col),
                ))

                base = dtype.rstrip("?")
                if base == "int":
                    def _ser(v): return None if pd.isna(v) else str(int(v))
                elif base == "float":
                    def _ser(v):
                        if pd.isna(v): return None
                        f = float(v)
                        return str(int(f)) if f.is_integer() else "{:.15g}".format(f)
                elif base == "datetime":
                    def _ser(v): return None if pd.isna(v) else pd.to_datetime(v).isoformat()
                elif base == "bool":
                    def _ser(v): return None if pd.isna(v) else ("true" if bool(v) else "false")
                else:
                    def _ser(v): return None if pd.isna(v) else str(v)
                serializers[col] = _ser

            session.add_all(column_objs)
            session.flush()  # assign IDs to column_objs

            col_id_by_name = {c.name: c.id for c in column_objs}

            # ---- write SourceData (bulk) ----
            objs = []
            CHUNK = 5000
            for row in df[raw_columns].itertuples(index=True, name=None):
                row_idx = int(row[0])
                values = row[1:]
                for col, val in zip(raw_columns, values):
                    sval = serializers[col](val)
                    objs.append(SourceData(
                        source_file_id=sf.id,
                        row_index=row_idx,
                        value=sval,
                        source_column_id=col_id_by_name[col],
                    ))
                if len(objs) >= CHUNK:
                    session.bulk_save_objects(objs)
                    objs.clear()

            if objs:
                session.bulk_save_objects(objs)

            session.commit()
            return {"status": "success"}

        except Exception as e:
            session.rollback()
            print(f"Data creation failed: {e}")
            raise
        finally:
            session.close()


    def get_source_data(self, source_file_id, offset=0, limit=100, sort_by=None, sort_dir="asc", filters=None):
        session = Session()
        try:
            source_file = session.get(SourceFile, source_file_id)
            if not source_file:
                return {"error": "Source file not found"}, 404

            # ---- columns metadata (ordered)
            col_rows = (
                session.query(SourceColumn)
                .filter_by(source_file_id=source_file_id)
                .order_by(SourceColumn.ordinal.asc())
                .all()
            )
            # maps & ordered lists
            cols = []
            name_to_base = {}
            name_to_id = {}
            id_to_name = {}
            col_ids_in_order = []
            col_names_in_order = []

            for c in col_rows:
                base, nullable = self.utilities.canon_dtype(c.data_type)
                cols.append({"name": c.name, "data_type": base, "nullable": nullable, "ordinal": c.ordinal})
                name_to_base[c.name] = base
                name_to_id[c.name] = c.id
                id_to_name[c.id] = c.name
                col_ids_in_order.append(c.id)
                col_names_in_order.append(c.name)

            # guard: if sort_by not a known column, disable sorting
            if sort_by and sort_by not in name_to_base:
                sort_by = None

            # ---- normalize & validate filters (case-insensitive substring on value)
            filters = filters or []
            active_filters = []
            for f in filters:
                col = (f or {}).get("col")
                text = str((f or {}).get("filter_text", "")).strip()
                if not col or col not in name_to_id or text == "":
                    continue
                active_filters.append({"col_id": name_to_id[col], "text": text})

            # helper to escape % and _ so they’re treated literally
            def _escape_like(s: str) -> str:
                return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

            # Subquery of matching row_index values if filters are present.
            # Rows must match ALL filters (one per column):
            # GROUP BY row_index HAVING COUNT(DISTINCT source_column_id) = N
            filtered_rows_subq = None
            if active_filters:
                or_clauses = []
                for f in active_filters:
                    pattern = f"%{_escape_like(f['text'])}%"
                    or_clauses.append(and_(
                        SourceData.source_column_id == f["col_id"],
                        SourceData.value.ilike(pattern, escape="\\"),
                    ))
                filtered_rows_q = (
                    session.query(SourceData.row_index)
                    .filter(SourceData.source_file_id == source_file_id)
                    .filter(or_(*or_clauses))
                    .group_by(SourceData.row_index)
                    .having(func.count(func.distinct(SourceData.source_column_id)) == len(active_filters))
                )
                filtered_rows_subq = filtered_rows_q.subquery()

            # ---- decide row indices (paging + optional sort), respecting filters
            if sort_by:
                sort_col_id = name_to_id.get(sort_by)
                if sort_col_id is None:
                    sort_by = None
                else:
                    base = name_to_base.get(sort_by, "string")
                    sort_value_expr = SourceData.value
                    if base == "int":
                        sort_value_expr = cast(SourceData.value, Integer)
                    elif base == "float":
                        sort_value_expr = cast(SourceData.value, Float)
                    elif base == "datetime":
                        sort_value_expr = cast(SourceData.value, DateTime)
                    elif base == "bool":
                        sort_value_expr = cast(SourceData.value, Boolean)

                    sort_order = sort_value_expr.desc().nullslast() if sort_dir == "desc" \
                                else sort_value_expr.asc().nullsfirst()

                    row_index_query = (
                        session.query(SourceData.row_index)
                        .filter(
                            SourceData.source_file_id == source_file_id,
                            SourceData.source_column_id == sort_col_id,
                        )
                    )
                    if filtered_rows_subq is not None:
                        row_index_query = row_index_query.filter(SourceData.row_index.in_(filtered_rows_subq))
                    row_index_query = row_index_query.order_by(
                        sort_order, SourceData.row_index.asc()
                    ).offset(offset).limit(limit)
            if not sort_by:
                row_index_query = (
                    session.query(SourceData.row_index)
                    .filter(SourceData.source_file_id == source_file_id)
                    .distinct()
                )
                if filtered_rows_subq is not None:
                    row_index_query = row_index_query.filter(SourceData.row_index.in_(filtered_rows_subq))
                row_index_query = row_index_query.order_by(SourceData.row_index.asc()).offset(offset).limit(limit)

            row_indices = [r.row_index for r in row_index_query.all()]

            # ---- total rows for pager (distinct row_index after filters)
            if filtered_rows_subq is not None:
                total_count = session.query(func.count()).select_from(filtered_rows_subq).scalar()
            else:
                total_count = (
                    session.query(SourceData.row_index)
                    .filter(SourceData.source_file_id == source_file_id)
                    .distinct()
                    .count()
                )

            if not row_indices:
                return {"cols": cols, "rows": [], "total": total_count}, 200

            # ---- fetch the cells for those rows (select only what you need)
            raw_rows = (
                session.query(SourceData.row_index, SourceData.source_column_id, SourceData.value)
                .filter(
                    SourceData.source_file_id == source_file_id,
                    SourceData.row_index.in_(row_indices),
                )
                .all()
            )

            # ---- group into records; preserve column order
            grouped = {}
            for row_index, col_id, val in raw_rows:
                grouped.setdefault(row_index, {})[col_id] = val

            rows = []
            for idx in row_indices:
                rec_src = grouped.get(idx, {})
                # build with names in display order
                row_obj = {name: rec_src.get(col_id) for name, col_id in zip(col_names_in_order, col_ids_in_order)}
                rows.append(row_obj)

            return {"cols": cols, "rows": rows, "total": total_count}, 200

        finally:
            session.close()


