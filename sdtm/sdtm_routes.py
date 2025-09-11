import json
from flask import Blueprint, jsonify, request
from sdtm.sdtm_service import SDTMService


sdtm_bp = Blueprint("sdtm_api", __name__)
sdtm_service = SDTMService()


@sdtm_bp.route("/sdtm/standards", methods=["GET"])
def get_sdtm_standards():
    include_domains = request.args.get("include_domains") == "true"
    return jsonify(sdtm_service.get_standards(include_domains=include_domains))


@sdtm_bp.route("/sdtm/standards/<int:standard_id>/domains/<string:domain_code>/variables", methods=["GET"])
def get_sdtm_domain_variables_by_code(standard_id, domain_code):
    data = sdtm_service.get_domain_variables_by_code(
        standard_id=standard_id, domain_code=domain_code
    )
    if not data.get("domain"):
        return jsonify({"error": "Domain not found for standard",
            "standard_id": standard_id,
            "domain_code": domain_code
        }), 404
    return jsonify(data)


@sdtm_bp.route("/sdtm/data", methods=["GET"])
def get_sdtm_data():
    domain = request.args.get("domain")
    source_file_id = request.args.get("source_file_id", type=int)
    mapping_schema_id = request.args.get("mapping_schema_id", type=int)
    offset = request.args.get("offset", default=0, type=int)
    limit = request.args.get("limit", default=100, type=int)
    sort_by = request.args.get("sort_by")
    sort_dir = request.args.get("sort_dir", default="asc")

    if not domain:
        return jsonify({"error": "domain required"}), 400

    filters_raw = request.args.get("filters")
    try:
        filters = json.loads(filters_raw) if filters_raw else []
    except json.JSONDecodeError:
        return jsonify({"error": "Invalid 'filters' param. Expect JSON array like [{\"col\":\"VSORRES\",\"filter_text\":\"n\"}]"}), 400

    result, status = sdtm_service.get_sdtm_data(
        domain=domain,
        source_file_id=source_file_id,
        mapping_schema_id=mapping_schema_id,
        offset=offset,
        limit=limit,
        sort_by=sort_by,
        sort_dir=sort_dir,
        filters=filters,
    )
    return jsonify(result), status

@sdtm_bp.route("/sdtm/overview", methods=["GET"])
def get_sdtm_overview():
    domain = request.args.get("domain")
    if not domain:
        return jsonify({"error": "domain required"}), 400

    source_file_id = request.args.get("source_file_id", type=int)
    mapping_schema_id = request.args.get("mapping_schema_id", type=int)
    stats = (request.args.get("stats") or "false").lower() in {"1", "true", "yes"}
    top_k = int(request.args.get("top_k") or 3)

    result = sdtm_service.get_sdtm_overview(
        domain=domain,
        source_file_id=source_file_id,
        mapping_schema_id=mapping_schema_id,
        stats=stats,
        top_k=top_k,
    )
    return jsonify(result), 200



