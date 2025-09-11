import os
import sys
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db import Session, MappingSchemaSourceFile, SourceData, SDTMData, SourceColumn
from mapper.transformer_utils import TransformerUtils
from mapper.modifications.modifications_service import ModificationsService
from mapper.modifications.translate import ui_mods_to_server_ops


class Transformer:
    def __init__(self):
        self.utils = TransformerUtils()
        self.modifier = ModificationsService()


    def run_transform(self, schema_id, source_file_id):
        """
        Execute SDTM transform for (schema_id, source_file_id).
        Returns a summary with row/cell counts per domain.
        """
        session = Session()
        try:
            # 1) Fetch mapping json from the link
            link = session.get(MappingSchemaSourceFile, (schema_id, source_file_id))
            if not link or not link.mapping_json:
                raise ValueError("No mapping_json found for this schema/file")

            mapping = link.mapping_json
            domains_cfg = mapping.get("domains") or []
            if not isinstance(domains_cfg, list) or not domains_cfg:
                raise ValueError("mapping_json.domains is empty or invalid")

            # 2) Load source data -> wide DataFrame
            src_q = (
                session.query(SourceData.row_index, SourceData.column_name, SourceData.value)
                .filter(
                    SourceData.source_file_id == source_file_id,
                    # If you support variants, set it here (e.g., variant == 'raw')
                )
            )
            rows = [(r.row_index, r.column_name, r.value) for r in src_q]
            if not rows:
                # still allow transform; it will just produce nothing
                df = pd.DataFrame(columns=["row_index"]).set_index("row_index")
            else:
                df = pd.DataFrame(rows, columns=["row_index", "column", "value"])
                df = df.pivot(index="row_index", columns="column", values="value")
                # keep row_index as an index (int), convenient for masking
                df.index.name = "row_index"

            # 3) Evaluate each domain/mapping and build outputs
            sdtm_insert_buffer = []
            stats = {}
            next_row_index_by_domain = {}

            for domain_cfg in domains_cfg:
                domain_code = (domain_cfg.get("domain") or "").upper()
                if not domain_code:
                    # skip invalid domain blocks
                    continue

                common_assign = (domain_cfg.get("common") or {}).get("assign") or []
                emitters = domain_cfg.get("emitters") or []

                domain_rows = 0
                domain_cells = 0

                for e in emitters:
                    where = e.get("where")

                    cols_meta = (
                        session.query(SourceColumn.name, SourceColumn.data_type)
                        .filter_by(source_file_id=source_file_id, variant="raw")
                        .all()
                    )
                    col_types = {name: dtype for (name, dtype) in cols_meta}
                    mask = self.utils.eval_where(df, where, col_types=col_types) if where else pd.Series(True, index=df.index)

                    if mask.sum() == 0:
                        continue

                    df_sub = df[mask]  # rows to transform for this mapping

                    # Build final assignment spec: common + mapping (no-override rule: drop duplicates from mapping)
                    final_assigns = self.utils.merge_assigns_no_override(common_assign, e.get("assign") or [])

                    # Evaluate each assign → returns a Series aligned to df_sub.index
                    values_by_to = {}
                    for a in final_assigns:
                        series = self.utils.eval_assign_series(df_sub, a)
                        # apply fallback if present and we have nulls/empties
                        fb = a.get("fallback")
                        if fb:
                            series = self.utils.apply_fallback(series, df_sub, fb)

                        ui_mods = a.get("mods") or []
                        if ui_mods:
                            ops, _ignored = ui_mods_to_server_ops(ui_mods)
                            if ops:
                                series = self.modifier.apply(series, ops)

                        # ensure dtype str (SDTMData.value is Text)
                        values_by_to[a["to"]] = series.astype(object).where(series.notna(), None)

                    # Now create SDTM rows
                    out_cols = list(values_by_to.keys())

                    # For each row, allocate an output row_index (per domain)
                    start_idx = next_row_index_by_domain.get(domain_code, 0)
                    out_row_indices = list(range(start_idx, start_idx + len(df_sub)))
                    next_row_index_by_domain[domain_code] = start_idx + len(df_sub)

                    # For each cell (row x column), create SDTMData row
                    for i, src_row_index in enumerate(df_sub.index):
                        sdtm_row_index = out_row_indices[i]
                        for col in out_cols:
                            val = values_by_to[col].iloc[i]
                            # stringify only if not None; keep None to store as NULL
                            sdtm_insert_buffer.append(SDTMData(
                                mapping_schema_id=schema_id,
                                domain=domain_code,
                                row_index=sdtm_row_index,
                                column_name=col,
                                value=None if val is None else str(val),
                                source_file_id=source_file_id,
                            ))
                            domain_cells += 1
                        domain_rows += 1

                # record stats for this domain
                if domain_code:
                    stats[domain_code] = {"rows": domain_rows, "cells": domain_cells}

            # 4) Delete previous SDTMData for these domains (by source_file_id)
            domains_to_clean = [ (d.get("domain") or "").upper() for d in domains_cfg if d.get("domain") ]
            if domains_to_clean:
                session.query(SDTMData).filter(
                    SDTMData.mapping_schema_id == schema_id,
                    SDTMData.source_file_id == source_file_id,
                    SDTMData.domain.in_(domains_to_clean),
                ).delete(synchronize_session=False)

            # 5) Insert new SDTMData
            if sdtm_insert_buffer:
                session.bulk_save_objects(sdtm_insert_buffer)

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



if __name__ == "__main__":
    transformer = Transformer()

    schema_id = 4
    source_file_id = 137

    result = transformer.run_transform(schema_id=schema_id, source_file_id=source_file_id)
    print(result)