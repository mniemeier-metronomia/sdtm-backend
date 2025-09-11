# tables/routes.py
from flask import Blueprint, request, jsonify
from project.project_service import ProjectService

project_bp = Blueprint("project_bp", __name__)
project_service = ProjectService()


@project_bp.route('/projects', methods=['GET'])
def get_projects():
    return jsonify(project_service.get_all_projects())


@project_bp.route('/projects', methods=['POST'])
def create_project():
    data = request.json
    if not data or 'name' not in data:
        return jsonify(error="Missing project name"), 400
    try:
        project = project_service.create_project(data['name'], data.get('description'))
        return jsonify(project)
    except Exception as e:
        print(f"Error creating project: {e}")
        return jsonify(error="Internal server error"), 500
