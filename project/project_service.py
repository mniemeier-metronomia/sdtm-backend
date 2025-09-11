import pandas as pd
import os
from sqlalchemy import cast, Integer, Float, Boolean, DateTime, func
import pyreadstat
import tempfile
from db import Session, Project


class ProjectService:
    def __init__(self):
        pass


    def get_all_projects(self):
        session = Session()
        try:
            projects = session.query(Project).order_by(Project.id).all()
            return [
                {
                    "id": p.id,
                    "name": p.name,
                    "description": p.description
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