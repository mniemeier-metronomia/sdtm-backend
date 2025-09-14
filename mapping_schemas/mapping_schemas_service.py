from db import Session, MappingSchema, MappingSchemaSourceFile, SDTMStandard
from sqlalchemy.orm import joinedload


class MappingSchemasService:

    def get_mapping_dict(self, m):
        std = getattr(m, "sdtm_standard", None)
        return {
            "id": m.id,
            "name": m.name,
            "version": m.version,
            "project_id": m.project_id,
            "status": m.status,
            "created_at": m.created_at.isoformat(),
            "updated_at": m.updated_at.isoformat(),
            "sdtm_standard": (
                {
                    "id": std.id,
                    "name": std.name,
                    "version": std.version,
                    "description": std.description,
                } if std else None
            ),
            "source_files": [
                {
                    "id": link.source_file_id,
                    "file_name": link.source_file.name if link.source_file else None,
                    "status": link.status,
                    "notes": link.notes,
                }
                for link in m.source_file_links
            ],
        }

    

    def get_mappings_for_project(self, project_id):
        session = Session()
        try:
            mappings = (
                session.query(MappingSchema)
                .options(
                    joinedload(MappingSchema.source_file_links).joinedload(MappingSchemaSourceFile.source_file),
                    joinedload(MappingSchema.sdtm_standard),
                )
                .filter_by(project_id=project_id)
                .order_by(MappingSchema.version.desc())
                .all()
            )
            return [self.get_mapping_dict(m) for m in mappings]
        finally:
            session.close()


    def create_mapping_schema(self, project_id, data):
        session = Session()
        try:
            sdtm_standard_id = data.get("sdtm_standard_id")
            if sdtm_standard_id is not None:
                std = session.query(SDTMStandard).get(sdtm_standard_id)
                if std is None:
                    return {"error": f"Unknown sdtm_standard_id: {sdtm_standard_id}"}, 400

            mapping = MappingSchema(
                project_id=project_id,
                name=data.get("name"),
                version=data.get("version", "v1"),
                status=data.get("status", "draft"),
                sdtm_standard_id=sdtm_standard_id,   # <-- new
            )
            session.add(mapping)
            session.commit()

            # refresh relationships for clean response
            session.refresh(mapping)
            return self.get_mapping_dict(mapping)
        finally:
            session.close()



    def delete_mapping_schema(self, mapping_id):
        session = Session()
        try:
            mapping = session.query(MappingSchema).get(mapping_id)
            if mapping:
                session.delete(mapping)
                session.commit()
        finally:
            session.close()


    def get_source_file_mapping_return_dict(self, link):
        return {
            "mapping_schema_id": link.mapping_schema_id,
            "source_file_id": link.source_file_id,
            "status": link.status,
            "notes": link.notes,
            "mapping_json": link.mapping_json,
            "created_at": getattr(link, "created_at", None).isoformat() if getattr(link, "created_at", None) else None,
            "updated_at": getattr(link, "updated_at", None).isoformat() if getattr(link, "updated_at", None) else None,
        }


    def get_source_file_mapping(self, schema_id, source_file_id):
        """
        Fetch the (schema_id, source_file_id) link row.
        Returns (found: bool, payload: dict). If not found, returns (False, {}).
        """
        session = Session()
        try:
            link = session.get(MappingSchemaSourceFile, (schema_id, source_file_id))
            if not link:
                return False, {}
            return True, self.get_source_file_mapping_return_dict(link)
        except Exception as e:
            session.rollback()
            print(f"Error: {e}")
            raise
        finally:
            session.close()


    def create_source_file_mapping(self, schema_id, source_file_id, mapping_json, status=None, notes=None):
        """
        Create link row (schema_id, source_file_id) with mapping_json.
        Returns (created, payload). If row exists, created=False and existing row is returned.
        """
        session = Session()
        try:
            existing = session.get(MappingSchemaSourceFile, (schema_id, source_file_id))
            if existing:
                return False, self.get_source_file_mapping_return_dict(existing)

            link = MappingSchemaSourceFile(
                mapping_schema_id=schema_id,
                source_file_id=source_file_id,
                mapping_json=mapping_json,
            )
            if status is not None:
                link.status = status
            if notes is not None:
                link.notes = notes

            session.add(link)
            session.commit()
            session.refresh(link)
            return True, self.get_source_file_mapping_return_dict(link)

        except Exception as e:
            session.rollback()
            print(f"Error: {e}")
            raise
        finally:
            session.close()


    def update_source_file_mapping(self, schema_id, source_file_id, mapping_json=None, status=None, notes=None):
        """
        Update mapping_json/status/notes for the (schema_id, source_file_id) link.
        Returns (updated, payload). If row not found, updated=False and {} payload.
        """
        session = Session()
        try:
            link = session.get(MappingSchemaSourceFile, (schema_id, source_file_id))
            if not link:
                return False, {}

            if mapping_json is not None:
                link.mapping_json = mapping_json
            if status is not None:
                link.status = status
            if notes is not None:
                link.notes = notes

            session.commit()
            session.refresh(link)
            return True, self.get_source_file_mapping_return_dict(link)

        except Exception as e:
            session.rollback()
            print(f"Error: {e}")
            raise
        finally:
            session.close()

