from flask import Blueprint, jsonify, request
from mapper.mapper_service import MapperService
from mapper.transformer import Transformer
from mapper.modifications.preview_mods_service import PreviewModsService
from mapper.modifications.value_map_suggest_service import ValueMapSuggestService


mapper_bp = Blueprint("mapper_bp", __name__)
mapper_service = MapperService()
transformer = Transformer()



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

