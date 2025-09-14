from db import Session, Project, SourceFile, engine
from sqlalchemy.orm import selectinload


class ProjectService:
    def __init__(self):
        pass


    def get_all_projects(self):
        session = Session()
        try:
            projects = (
                session.query(Project)
                .options(
                    selectinload(Project.source_files).load_only(SourceFile.id, SourceFile.name)
                )
                .order_by(Project.id)
                .all()
            )
            return [
                {
                    "id": p.id,
                    "name": p.name,
                    "description": p.description,
                    "source_file_names": [sf.name for sf in p.source_files],
                    "source_files_count": len(p.source_files),
                }
                for p in projects
            ]
        finally:
            session.close()


    def create_project(self, name, description=None):
        session = Session()
        try:
            project = Project(name=name, description=description)
            session.add(project)
            session.commit()
            session.refresh(project)
            return {
                "id": project.id,
                "name": project.name,
                "description": project.description,
            }
        except Exception as e:
            session.rollback()
            print(f"Error creating project: {e}")
            raise
        finally:
            session.close()


    def update_project(self, project_id, name=None, description=None):
        session = Session()
        try:
            project = session.query(Project).get(project_id)
            if project is None:
                return None

            if name is not None:
                project.name = name
            if description is not None:
                project.description = description

            session.commit()
            session.refresh(project)
            return {
                "id": project.id,
                "name": project.name,
                "description": project.description,
            }
        except Exception as e:
            session.rollback()
            print(f"Error updating project: {e}")
            raise
        finally:
            session.close()


    def delete_project(self, project_id):
        session = Session()
        raw_conn = engine.raw_connection()
        try:
            project = session.query(Project).get(project_id)
            if project is None:
                return False

            # Collect all file_oids in this project
            oids = [
                oid for (oid,) in session.query(SourceFile.file_oid)
                .filter(
                    SourceFile.project_id == project_id,
                    SourceFile.file_oid.isnot(None)
                ).all()
            ]

            # Unlink each LO (ignore if already gone)
            try:
                for oid in oids:
                    try:
                        raw_conn.lobject(oid).unlink()
                    except Exception:
                        pass
                raw_conn.commit()
            except Exception:
                raw_conn.rollback()
                raise

            # DB-level cascades will remove SourceFile, SourceColumn, SourceData, etc.
            session.delete(project)
            session.commit()
            return True
        except Exception:
            session.rollback()
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()
            session.close()
