import csv
from io import StringIO, BytesIO
from db import Session, SDTMColumn, SDTMVariable, SDTMDomain, SDTMData


class SDTMDownloadService:
    # ------------- helpers -------------
    def _sdtm_headers_for_scope(self, session, domain, mapping_schema_id, source_file_id=None):
        """Return (ordered_vars, meta_by_name, col_ids_by_var, include_source_file_possible)"""
        dom = (domain or "").upper().strip()
        hdr_q = (
            session.query(
                SDTMColumn.id.label("col_id"),
                SDTMColumn.source_file_id.label("sfid"),
                SDTMVariable.name.label("var_name"),
                SDTMVariable.data_type.label("ig_type"),
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

        rows = hdr_q.order_by(SDTMVariable.variable_order.asc(), SDTMVariable.name.asc()).all()
        if not rows:
            return [], {}, {}, False

        meta_by_name = {}
        col_ids_by_var = {}
        multi_file = False
        seen_sf = set()

        for r in rows:
            seen_sf.add(r.sfid)
            if r.var_name not in meta_by_name:
                dt = "num" if (r.ig_type or "").lower().startswith("num") else "text"
                meta_by_name[r.var_name] = {"ordinal": r.var_order, "data_type": dt}
            col_ids_by_var.setdefault(r.var_name, set()).add(r.col_id)

        multi_file = (len(seen_sf) > 1 and source_file_id is None)
        ordered_vars = sorted(meta_by_name.keys(), key=lambda n: meta_by_name[n]["ordinal"])
        return ordered_vars, meta_by_name, col_ids_by_var, multi_file

    # ------------- CSV (streaming) -------------
    def stream_sdtm_csv(self, domain, mapping_schema_id, source_file_id=None, include_source_file=False):
        """Yield a CSV for the full SDTM scope; memory-safe streaming."""
        session = Session()
        try:
            ordered_vars, meta_by_name, col_ids_by_var, multi_file = self._sdtm_headers_for_scope(
                session, domain, mapping_schema_id, source_file_id
            )
            if not ordered_vars:
                # header only (or empty file)
                def _empty():
                    buf = StringIO()
                    w = csv.writer(buf)
                    header = (["SOURCE_FILE_ID"] if include_source_file else []) + ordered_vars
                    w.writerow(header)
                    yield buf.getvalue()
                return _empty()

            # Header
            def row_iter():
                buf = StringIO()
                w = csv.writer(buf)
                header = (["SOURCE_FILE_ID"] if (include_source_file or (multi_file and source_file_id is None)) else []) + ordered_vars
                w.writerow(header)
                yield buf.getvalue(); buf.seek(0); buf.truncate(0)

                # Query all cells ordered by (sfid, row_index, variable_order)
                q = (
                    session.query(
                        SDTMColumn.source_file_id.label("sfid"),
                        SDTMData.row_index.label("ri"),
                        SDTMVariable.variable_order.label("ord"),
                        SDTMVariable.name.label("var_name"),
                        SDTMData.value.label("val"),
                    )
                    .join(SDTMVariable, SDTMVariable.id == SDTMColumn.sdtm_variable_id)
                    .join(SDTMDomain, SDTMDomain.id == SDTMVariable.domain_id)
                    .join(SDTMData, SDTMData.sdtm_column_id == SDTMColumn.id)
                    .filter(
                        SDTMColumn.mapping_schema_id == mapping_schema_id,
                        SDTMDomain.name == domain.upper(),
                    )
                    .order_by(SDTMColumn.source_file_id.asc(), SDTMData.row_index.asc(), SDTMVariable.variable_order.asc())
                )
                if source_file_id is not None:
                    q = q.filter(SDTMColumn.source_file_id == source_file_id)

                # assemble rows on the fly
                current_key = None
                row_dict = {}

                for sfid, ri, ord_val, var_name, val in q.yield_per(10000):
                    key = (sfid, ri)
                    if current_key is None:
                        current_key = key
                        row_dict = {}

                    if key != current_key:
                        # emit previous
                        row = ([current_key[0]] if (include_source_file or (multi_file and source_file_id is None)) else []) \
                              + [row_dict.get(v, "") for v in ordered_vars]
                        w.writerow(row)
                        yield buf.getvalue(); buf.seek(0); buf.truncate(0)
                        current_key = key
                        row_dict = {}

                    row_dict[var_name] = "" if val is None else str(val)

                if current_key is not None:
                    row = ([current_key[0]] if (include_source_file or (multi_file and source_file_id is None)) else []) \
                          + [row_dict.get(v, "") for v in ordered_vars]
                    w.writerow(row)
                    yield buf.getvalue()

            return row_iter()
        finally:
            session.close()

    # ------------- XLSX (in memory) -------------
    def build_sdtm_xlsx(self, domain, mapping_schema_id, source_file_id=None, include_source_file=False, sheet_name="Sheet1"):
        """Return BytesIO of the built XLSX for full SDTM scope."""
        session = Session()
        try:
            ordered_vars, meta_by_name, col_ids_by_var, multi_file = self._sdtm_headers_for_scope(
                session, domain, mapping_schema_id, source_file_id
            )
            # Prepare workbook
            out = BytesIO()
            import xlsxwriter
            wb = xlsxwriter.Workbook(out, {"in_memory": True})
            ws = wb.add_worksheet(sheet_name)

            # Header
            cols = (["SOURCE_FILE_ID"] if (include_source_file or (multi_file and source_file_id is None)) else []) + ordered_vars
            for ci, name in enumerate(cols):
                ws.write(0, ci, name)

            # Query cells ordered, fill rows
            q = (
                session.query(
                    SDTMColumn.source_file_id.label("sfid"),
                    SDTMData.row_index.label("ri"),
                    SDTMVariable.variable_order.label("ord"),
                    SDTMVariable.name.label("var_name"),
                    SDTMData.value.label("val"),
                )
                .join(SDTMVariable, SDTMVariable.id == SDTMColumn.sdtm_variable_id)
                .join(SDTMDomain, SDTMDomain.id == SDTMVariable.domain_id)
                .join(SDTMData, SDTMData.sdtm_column_id == SDTMColumn.id)
                .filter(
                    SDTMColumn.mapping_schema_id == mapping_schema_id,
                    SDTMDomain.name == domain.upper(),
                )
                .order_by(SDTMColumn.source_file_id.asc(), SDTMData.row_index.asc(), SDTMVariable.variable_order.asc())
            )
            if source_file_id is not None:
                q = q.filter(SDTMColumn.source_file_id == source_file_id)

            # column index map
            col_index = {name: idx for idx, name in enumerate(cols)}
            rownum = 1
            current_key = None
            row_vals = {}

            for sfid, ri, ord_val, var_name, val in q.yield_per(10000):
                key = (sfid, ri)
                if current_key is None:
                    current_key = key
                    row_vals = {}

                if key != current_key:
                    # flush previous row
                    if include_source_file or (multi_file and source_file_id is None):
                        ws.write(rownum, 0, current_key[0])
                    for name in ordered_vars:
                        ci = col_index[name] if (include_source_file or (multi_file and source_file_id is None)) else col_index[name]
                        ws.write(rownum, ci, row_vals.get(name))
                    rownum += 1
                    current_key = key
                    row_vals = {}

                row_vals[var_name] = "" if val is None else str(val)

            # flush last row
            if current_key is not None:
                if include_source_file or (multi_file and source_file_id is None):
                    ws.write(rownum, 0, current_key[0])
                for name in ordered_vars:
                    ci = col_index[name] if (include_source_file or (multi_file and source_file_id is None)) else col_index[name]
                    ws.write(rownum, ci, row_vals.get(name))
                rownum += 1

            wb.close()
            out.seek(0)
            return out
        finally:
            session.close()


