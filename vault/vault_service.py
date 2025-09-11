from werkzeug.utils import secure_filename
import pandas as pd
import io, os
from db import Session, engine, SourceFile


class VaultService:

    def upload_file(self, project_id, file, name):
        session = Session()
        raw_conn = engine.raw_connection()
        try:
            # Save to large object
            with raw_conn.cursor() as cursor:
                lo = raw_conn.lobject(0, mode='wb')
                file_bytes = file.read()
                lo.write(file_bytes)
                file_oid = lo.oid
                lo.close()
            raw_conn.commit()

            # Try loading into pandas
            num_rows = None
            num_columns = None
            try:
                filename = file.filename.lower()
                buf = io.BytesIO(file_bytes)

                if filename.endswith(".csv"):
                    df = pd.read_csv(buf, nrows=1000)  # sample or read full
                    num_rows, num_columns = df.shape
                    # if you want full row count without loading all:
                    buf.seek(0)
                    num_rows = sum(1 for _ in buf) - 1  # naive CSV line count
                elif filename.endswith(".sas7bdat"):
                    df = pd.read_sas(buf, format="sas7bdat")
                    num_rows, num_columns = df.shape
                elif filename.endswith((".xls", ".xlsx")):
                    df = pd.read_excel(buf)
                    num_rows, num_columns = df.shape
                else:
                    # fallback: leave nulls
                    pass
            except Exception as e:
                # don’t block file save if pandas fails
                print("Warning: failed to parse file for stats", e)

            # Save metadata row
            source_file = SourceFile(
                project_id=project_id,
                name=name,
                content_type=file.content_type or "application/octet-stream",
                file_oid=file_oid,
                num_rows=num_rows,
                num_columns=num_columns,
            )
            session.add(source_file)
            session.commit()

            return {
                "id": source_file.id,
                "name": source_file.name,
                "uploaded_at": source_file.uploaded_at.isoformat(),
                "num_rows": source_file.num_rows,
                "num_columns": source_file.num_columns,
            }

        except Exception:
            session.rollback()
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()
            session.close()


    def delete_file(self, file_id):
        session = Session()
        raw_conn = engine.raw_connection()
        try:
            source_file = session.query(SourceFile).get(file_id)
            if not source_file:
                return False

            # Delete large object
            if source_file.file_oid:
                with raw_conn.cursor() as cursor:
                    try:
                        raw_conn.lobject(source_file.file_oid).unlink()
                    except Exception:
                        pass

            session.delete(source_file)
            session.commit()
            return True
        except Exception as e:
            print(f"Error: {e}")
            session.rollback()
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()
            session.close()
