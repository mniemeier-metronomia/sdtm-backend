import os
import sys
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db import (
    Session,
    MappingSchemaSourceFile,
    MappingSchema,
    SourceData,
    SourceColumn,
    SDTMData,
    SDTMColumn,
    SDTMVariable,
    SDTMDomain,
)
from mapper.transformer_utils import TransformerUtils
from mapper.modifications.modifications_service import ModificationsService
from mapper.modifications.translate import ui_mods_to_server_ops


class Transformer:
    def __init__(self):
        self.utils = TransformerUtils()
        self.modifier = ModificationsService()

    def run_transform(self, schema_id: int, source_file_id: int):
        """
        Full rebuild for (schema_id, source_file_id):
          - delete all SDTMColumn for this (schema,file) -> DB CASCADE deletes SDTMData
          - read SourceData -> wide DataFrame
          - for each mapped domain/emitter: evaluate assignments, create headers (SDTMColumn) as needed,
            and write SDTMData cells (sdtm_column_id, row_index, value)
        Returns stats by domain.
        """
        session = Session()
        try:
            # 1) mapping + schema/standard
            link = session.get(MappingSchemaSourceFile, (schema_id, source_file_id))
            if not link or not link.mapping_json:
                raise ValueError("No mapping_json found for this schema/file")
            mapping = link.mapping_json

            ms = session.get(MappingSchema, schema_id)
            if not ms or not ms.sdtm_standard_id:
                raise ValueError("Mapping schema or its SDTM standard not found")

            domains_cfg = mapping.get("domains") or []
            if not isinstance(domains_cfg, list) or not domains_cfg:
                # nothing to do, but still clear previous output
                session.query(SDTMColumn).filter(
                    SDTMColumn.mapping_schema_id == schema_id,
                    SDTMColumn.source_file_id == source_file_id,
                ).delete(synchronize_session=False)
                session.commit()
                return {"schema_id": schema_id, "source_file_id": source_file_id, "domains": {}, "total_rows": 0, "total_cells": 0}

            # 2) **FULL CLEAN** previous output for this (schema,file)
            session.query(SDTMColumn).filter(
                SDTMColumn.mapping_schema_id == schema_id,
                SDTMColumn.source_file_id == source_file_id,
            ).delete(synchronize_session=False)  # ON DELETE CASCADE removes SDTMData

            # 3) Source data -> wide DF (join to SourceColumn for names)
            src_rows = (
                session.query(SourceData.row_index, SourceColumn.name, SourceData.value)
                .join(SourceColumn, SourceColumn.id == SourceData.source_column_id)
                .filter(SourceColumn.source_file_id == source_file_id)
                .all()
            )
            if not src_rows:
                df = pd.DataFrame(columns=["row_index"]).set_index("row_index")
            else:
                df = pd.DataFrame(src_rows, columns=["row_index", "column", "value"]).pivot(
                    index="row_index", columns="column", values="value"
                )
                df.index.name = "row_index"

            # source column dtype map for WHERE evaluation
            cols_meta = (
                session.query(SourceColumn.name, SourceColumn.data_type)
                .filter_by(source_file_id=source_file_id)
                .all()
            )
            col_types = {name: dtype for (name, dtype) in cols_meta}

            # 4) Preload IG variables for all domains present in mapping (for fast lookups)
            target_domains = sorted({(d.get("domain") or "").upper() for d in domains_cfg if d.get("domain")})
            var_rows = []
            if target_domains:
                var_rows = (
                    session.query(SDTMVariable.id, SDTMVariable.name, SDTMDomain.name)
                    .join(SDTMDomain, SDTMDomain.id == SDTMVariable.domain_id)
                    .filter(
                        SDTMDomain.standard_id == ms.sdtm_standard_id,
                        SDTMDomain.name.in_(target_domains),
                    )
                    .all()
                )
            var_id_by_dom_var = {(dom, var): vid for (vid, var, dom) in var_rows}

            # existing headers (will be empty after delete, but keep map to avoid dupes in-loop)
            col_id_by_varid = {}

            stats = {}
            next_row_index_by_domain = {}
            CHUNK = 5000

            # 5) Transform per domain
            missing_vars = []
            for domain_cfg in domains_cfg:
                domain_code = (domain_cfg.get("domain") or "").upper()
                if not domain_code:
                    continue

                common_assign = (domain_cfg.get("common") or {}).get("assign") or []
                emitters = domain_cfg.get("emitters") or []

                domain_rows = 0
                domain_cells = 0

                for e in emitters:
                    where = e.get("where")
                    mask = self.utils.eval_where(df, where, col_types=col_types) if where else pd.Series(True, index=df.index)
                    if mask.sum() == 0:
                        continue
                    df_sub = df[mask]

                    # merge assigns (no-override)
                    final_assigns = self.utils.merge_assigns_no_override(common_assign, e.get("assign") or [])

                    # evaluate each assign to a Series aligned to df_sub
                    values_by_to = {}
                    for a in final_assigns:
                        series = self.utils.eval_assign_series(df_sub, a)
                        fb = a.get("fallback")
                        if fb:
                            series = self.utils.apply_fallback(series, df_sub, fb)
                        ui_mods = a.get("mods") or []
                        if ui_mods:
                            ops, _ = ui_mods_to_server_ops(ui_mods)
                            if ops:
                                series = self.modifier.apply(series, ops)
                        values_by_to[a["to"]] = series.astype(object).where(series.notna(), None)

                    out_vars = list(values_by_to.keys())
                    if not out_vars:
                        continue

                    # resolve IG variable ids and ensure SDTMColumn headers exist
                    sdtm_col_id_for_var = {}
                    for var_name in out_vars:
                        key = (domain_code, var_name)
                        var_id = var_id_by_dom_var.get(key)
                        if not var_id:
                            missing_vars.append(key)
                            continue
                        col_id = col_id_by_varid.get(var_id)
                        if not col_id:
                            new_col = SDTMColumn(
                                mapping_schema_id=schema_id,
                                source_file_id=source_file_id,
                                sdtm_variable_id=var_id,
                            )
                            session.add(new_col)
                            session.flush()  # assign id
                            col_id = new_col.id
                            col_id_by_varid[var_id] = col_id
                        sdtm_col_id_for_var[var_name] = col_id

                    if missing_vars:
                        uniq = sorted(set(missing_vars))
                        raise ValueError(f"Unknown SDTM variables for standard {ms.sdtm_standard_id}: {uniq}")

                    # allocate row indices for this domain
                    start_idx = next_row_index_by_domain.get(domain_code, 0)
                    out_row_indices = list(range(start_idx, start_idx + len(df_sub)))
                    next_row_index_by_domain[domain_code] = start_idx + len(df_sub)

                    # emit cells (bulk, chunked)
                    objs = []
                    for i, _src_ri in enumerate(df_sub.index):
                        sdtm_ri = out_row_indices[i]
                        for var_name in out_vars:
                            col_id = sdtm_col_id_for_var[var_name]
                            val = values_by_to[var_name].iloc[i]
                            objs.append(SDTMData(
                                row_index=sdtm_ri,
                                value=None if val is None else str(val),
                                sdtm_column_id=col_id,
                            ))
                            domain_cells += 1
                        domain_rows += 1

                        if len(objs) >= CHUNK:
                            session.bulk_save_objects(objs)
                            objs.clear()
                    if objs:
                        session.bulk_save_objects(objs)

                stats[domain_code] = {"rows": domain_rows, "cells": domain_cells}

            session.commit()

            total_rows = sum(v["rows"] for v in stats.values())
            total_cells = sum(v["cells"] for v in stats.values())

            return {
                "schema_id": schema_id,
                "source_file_id": source_file_id,
                "domains": stats,
                "total_rows": total_rows,
                "total_cells": total_cells,
            }

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
