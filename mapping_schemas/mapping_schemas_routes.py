from flask import Blueprint, jsonify, request
from mapping_schemas.mapping_schemas_service import MappingSchemasService


mapping_schema_bp = Blueprint("mapping_schema_bp", __name__)
mapping_schemas_service = MappingSchemasService()


# Get all mapping schemas for a project
@mapping_schema_bp.route("/mapping-schemas/projects/<int:project_id>", methods=["GET"])
def get_mappings(project_id):
    return jsonify(mapping_schemas_service.get_mappings_for_project(project_id))


# Create a new mapping schema
@mapping_schema_bp.route("/mapping-schemas/projects/<int:project_id>", methods=["POST"])
def create_mapping(project_id):
    data = request.json
    return jsonify(mapping_schemas_service.create_mapping_schema(project_id, data))


# Delete a mapping schema
@mapping_schema_bp.route("/mapping-schemas/<int:schema_id>", methods=["DELETE"])
def delete_mapping(schema_id):
    mapping_schemas_service.delete_mapping_schema(schema_id)
    return jsonify({"success": True})


@mapping_schema_bp.route("/mapping-schemas/<int:schema_id>/files/<int:source_file_id>/mapping", methods=["GET"])
def get_source_file_mapping(schema_id, source_file_id):
    found, row = mapping_schemas_service.get_source_file_mapping(schema_id=schema_id, source_file_id=source_file_id)
    if not found:
        return jsonify({"error": "Mapping not found for this schema/file"}), 404
    return jsonify(row), 200


@mapping_schema_bp.route("/mapping-schemas/<int:schema_id>/files/<int:source_file_id>/mapping", methods=["POST"])
def create_source_file_mapping(schema_id, source_file_id):
    """
    Create the mapping_json row for (schema_id, source_file_id).
    201 if created, 409 if it already exists.
    Body: { "mapping_json": { ... }, "status": "optional_status" }
    """
    data = request.get_json()
    mapping_json = data.get("mapping_json")
    status = data.get("status", None)
    notes = data.get("notes", None)

    if mapping_json is None:
        return jsonify({"error": "mapping_json is required"}), 400

    created, row = mapping_schemas_service.create_source_file_mapping(
        schema_id=schema_id,
        source_file_id=source_file_id,
        mapping_json=mapping_json,
        status=status,
        notes=notes,
    )

    if not created:
        return jsonify({"error": "Mapping already exists for this schema/file"}), 409

    return jsonify(row), 201


@mapping_schema_bp.route("/mapping-schemas/<int:schema_id>/files/<int:source_file_id>/mapping", methods=["PATCH", "PUT"])
def update_source_file_mapping(schema_id, source_file_id):
    """
    Update the mapping_json row for (schema_id, source_file_id).
    200 if updated, 404 if row does not exist.
    Body: { "mapping_json": { ... }, "status": "optional_status" }
    (Either field may be provided; if omitted it won't be changed.)
    """
    data = request.get_json()
    mapping_json = data.get("mapping_json", None)
    status = data.get("status", None)
    notes = data.get("notes", None)

    updated, row = mapping_schemas_service.update_source_file_mapping(
        schema_id=schema_id,
        source_file_id=source_file_id,
        mapping_json=mapping_json,
        status=status,
        notes=notes,
    )

    if not updated:
        return jsonify({"error": "Mapping not found for this schema/file"}), 404

    return jsonify(row), 200