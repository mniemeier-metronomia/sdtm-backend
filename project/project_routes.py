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


@project_bp.route('/projects/<int:project_id>', methods=['PUT', 'PATCH'])
def update_project(project_id):
    data = request.json or {}
    # Accept only name/description; ignore other keys
    payload = {k: v for k, v in data.items() if k in {"name", "description"}}

    if not payload:
        return jsonify(error="Nothing to update. Provide 'name' and/or 'description'."), 400

    # Optional: reject empty name if provided
    if "name" in payload and (payload["name"] is None or str(payload["name"]).strip() == ""):
        return jsonify(error="Project name cannot be empty."), 400

    try:
        updated = project_service.update_project(
            project_id,
            name=payload.get("name"),
            description=payload.get("description"),
        )
        if updated is None:
            return jsonify(error="Project not found"), 404
        return jsonify(updated), 200
    except Exception as e:
        print(f"Error updating project {project_id}: {e}")
        return jsonify(error="Internal server error"), 500
    

@project_bp.route('/projects/<int:project_id>', methods=['DELETE'])
def delete_project(project_id):
    try:
        deleted = project_service.delete_project(project_id)
        if not deleted:
            return jsonify(error="Project not found"), 404
        # No body needed for a successful delete
        return "", 204
    except Exception as e:
        print(f"Error deleting project {project_id}: {e}")
        return jsonify(error="Internal server error"), 500

