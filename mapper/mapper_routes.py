from flask import Blueprint, jsonify, request
from mapper.mapper_service import MapperService
from mapper.transformer import Transformer
from mapper.modifications.preview_mods_service import PreviewModsService
from mapper.modifications.value_map_suggest_service import ValueMapSuggestService


mapper_bp = Blueprint("mapper_bp", __name__)
mapper_service = MapperService()
transformer = Transformer()


# Get all mapping schemas for a project
@mapper_bp.route("/mapper/projects/<int:project_id>/mappings", methods=["GET"])
def get_mappings(project_id):
    return jsonify(mapper_service.get_mappings_for_project(project_id))


# Create a new mapping schema
@mapper_bp.route("/mapper/projects/<int:project_id>/mappings", methods=["POST"])
def create_mapping(project_id):
    data = request.json
    return jsonify(mapper_service.create_mapping_schema(project_id, data))


# Delete a mapping schema
@mapper_bp.route("/mapper/mappings/<int:schema_id>", methods=["DELETE"])
def delete_mapping(schema_id):
    mapper_service.delete_mapping_schema(schema_id)
    return jsonify({"success": True})


@mapper_bp.route("/mapper/schemas/<int:schema_id>/files/<int:source_file_id>/mapping", methods=["GET"])
def get_source_file_mapping(schema_id, source_file_id):
    found, row = mapper_service.get_source_file_mapping(schema_id=schema_id, source_file_id=source_file_id)
    if not found:
        return jsonify({"error": "Mapping not found for this schema/file"}), 404
    return jsonify(row), 200


@mapper_bp.route("/mapper/schemas/<int:schema_id>/files/<int:source_file_id>/mapping", methods=["POST"])
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

    created, row = mapper_service.create_source_file_mapping(
        schema_id=schema_id,
        source_file_id=source_file_id,
        mapping_json=mapping_json,
        status=status,
        notes=notes,
    )

    if not created:
        return jsonify({"error": "Mapping already exists for this schema/file"}), 409

    return jsonify(row), 201


@mapper_bp.route("/mapper/schemas/<int:schema_id>/files/<int:source_file_id>/mapping", methods=["PATCH", "PUT"])
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

    updated, row = mapper_service.update_source_file_mapping(
        schema_id=schema_id,
        source_file_id=source_file_id,
        mapping_json=mapping_json,
        status=status,
        notes=notes,
    )

    if not updated:
        return jsonify({"error": "Mapping not found for this schema/file"}), 404

    return jsonify(row), 200


@mapper_bp.post("/mapper/files/<int:source_file_id>/preview-modifications")
def preview_mods(source_file_id):
    data = request.get_json(silent=True) or {}
    assign = data.get("assign") or {}       # { mode, value, fallback? , to? (ignored) }
    where = data.get("where")               # optional domain where-tree
    mods = data.get("mods") or []           # UI mods as-is
    top_n = int(data.get("top_n", 20))
    max_rows = int(data.get("max_rows", 5000))

    if not isinstance(assign, dict) or not assign.get("mode"):
        return jsonify({"error": "Missing or invalid 'assign'."}), 400

    svc = PreviewModsService()
    try:
        result = svc.preview_assign_modifications(
            source_file_id=source_file_id,
            assign=assign,
            mods=mods,
            where=where,
            top_n=top_n,
            max_rows=max_rows,
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@mapper_bp.post("/mapper/files/<int:source_file_id>/suggest-value-map")
def suggest_value_map(source_file_id):
    data = request.get_json(silent=True) or {}
    assign = data.get("assign") or {}
    where = data.get("where")
    match_options = data.get("match_options") or {}  # { trim: bool, case_sensitive: bool }
    top_n = int(data.get("top_n", 200))
    max_rows = int(data.get("max_rows", 5000))

    standard_id = data.get("standard_id")
    domain = data.get("domain")
    variable = data.get("variable") or assign.get("to")

    if not assign.get("mode"):
        return jsonify({"error": "Missing or invalid 'assign'."}), 400

    svc = ValueMapSuggestService()
    try:
        result = svc.suggest(
            source_file_id=source_file_id,
            assign=assign,
            where=where,
            match_options=match_options,
            top_n=top_n,
            max_rows=max_rows,
            standard_id=standard_id,
            domain=domain,
            variable=variable,
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    

@mapper_bp.route("/mapper/schemas/<int:schema_id>/files/<int:source_file_id>/transform", methods=["POST"])
def transform(schema_id, source_file_id):
    try:
        result = transformer.run_transform(schema_id, source_file_id)
        return jsonify(result), 200
    except Exception as e:
        print(f"Transform error: {e}")
        return jsonify({"error": str(e)}), 500

