import json
from flask import Blueprint, jsonify, request
from source_files.source_files_service import SourceFilesService

source_files_bp = Blueprint("source_files_bp", __name__)

source_files_service = SourceFilesService()


@source_files_bp.route("/projects/<int:project_id>/source-files", methods=["GET"])
def get_project_source_files_overview(project_id):
    try:
        result = source_files_service.get_project_overview(project_id)
        return jsonify(result), 200
    except Exception as e:
        print(f"Error fetching project overview: {e}")
        return jsonify({"error": "Internal server error"}), 500
       

@source_files_bp.route("/source-files/<int:sourcefile_id>/overview", methods=["GET"])
def get_overview(sourcefile_id):
    stats = (request.args.get("stats") or "false").lower() in {"1", "true", "yes"}
    top_k = int(request.args.get("top_k") or 3)
    try:
        overview = source_files_service.get_overview(sourcefile_id, stats=stats, top_k=top_k)
        return jsonify(overview), 200
    except Exception as e:
        print(f"Error fetching source file overview: {e}")
        return jsonify({"error": str(e)}), 500
  

@source_files_bp.route("/source-files/<int:sourcefile_id>/check-keys", methods=["POST"])
def check_key_combination(sourcefile_id):
    try:
        key_columns = request.json.get("columns", [])
        result = source_files_service.check_key_uniqueness(sourcefile_id, key_columns)
        return jsonify(result), 200
    except Exception as e:
        print(f"Key uniqueness check failed: {e}")
        return jsonify({"error": str(e)}), 400


@source_files_bp.route("/source-files/<int:sourcefile_id>", methods=["PATCH"])
def update_source_file(sourcefile_id):
    try:
        data = request.get_json(force=True) or {}
        result = source_files_service.update_source_file(
            source_file_id=sourcefile_id,
            key_columns=data.get("key_columns", None),
            included_columns=data.get("included_columns", None)
        )
        return jsonify(result), 200
    except Exception as e:
        print(f"error: {e}")
        return jsonify({"error": str(e)}), 500


@source_files_bp.route("/source-files/<int:source_file_id>/generate-data", methods=["POST"])
def generate_source_data(source_file_id):
    try:
        result = source_files_service.generate_source_data(source_file_id)
        return jsonify(result), 200
    except Exception as e:
        print(f"Error generating data: {e}")
        return jsonify({"error": str(e)}), 500
   

@source_files_bp.route("/source-files/<int:source_file_id>/data", methods=["GET"])
def get_source_data(source_file_id):
    try:
        offset   = request.args.get("offset", default=0, type=int)
        limit    = request.args.get("limit", default=100, type=int)
        sort_by  = request.args.get("sort_by")
        sort_dir = request.args.get("sort_dir", default="asc")

        filters_raw = request.args.get("filters")  # JSON stringified array
        try:
            filters = json.loads(filters_raw) if filters_raw else []
        except json.JSONDecodeError:
            return jsonify({"error": "Invalid 'filters' param. Expect JSON array like [{\"col\":\"VSPERF_STD\",\"filter_text\":\"n\"}]"}), 400

        result, status = source_files_service.get_source_data(
            source_file_id=source_file_id,
            offset=offset,
            limit=limit,
            sort_by=sort_by,
            sort_dir=sort_dir,
            filters=filters,
        )
        return jsonify(result), status

    except Exception as e:
        print(f"Error fetching source table data: {e}")
        return jsonify({"error": str(e)}), 500

