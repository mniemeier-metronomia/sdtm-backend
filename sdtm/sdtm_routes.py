import json
from flask import Blueprint, jsonify, request, send_file, Response
from sdtm.sdtm_service import SDTMService
from sdtm.sdtm_download_service import SDTMDownloadService

sdtm_bp = Blueprint("sdtm_api", __name__)
sdtm_service = SDTMService()
sdtm_download_service = SDTMDownloadService()


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
    if mapping_schema_id is None:
        return jsonify({"error": "mapping_schema_id required"}), 400

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
    source_file_id = request.args.get("source_file_id", type=int)
    mapping_schema_id = request.args.get("mapping_schema_id", type=int)
    stats = (request.args.get("stats") or "false").lower() in {"1", "true", "yes"}
    top_k = int(request.args.get("top_k") or 3)

    if not domain:
        return jsonify({"error": "domain required"}), 400
    if not mapping_schema_id:
        return jsonify({"error": "mapping_schema_id required"}), 400

    

    result = sdtm_service.get_sdtm_overview(
        domain=domain,
        source_file_id=source_file_id,
        mapping_schema_id=mapping_schema_id,
        stats=stats,
        top_k=top_k,
    )
    return jsonify(result), 200


@sdtm_bp.route("/sdtm/mapped-domains", methods=["GET"])
def get_mapped_domains():
    mapping_schema_id = request.args.get("mapping_schema_id", type=int)

    if mapping_schema_id is None:
        return jsonify({"error": "mapping_schema_id required"}), 400

    result = sdtm_service.get_mapped_domains(
        mapping_schema_id=mapping_schema_id,
    )

    return jsonify(result), 200


@sdtm_bp.get("/sdtm/export")
def export_sdtm():
    domain = request.args.get("domain")
    mapping_schema_id = request.args.get("mapping_schema_id", type=int)
    source_file_id = request.args.get("source_file_id", type=int)  # optional
    fmt = (request.args.get("fmt") or "csv").lower()               # csv|xlsx
    include_source_file = (request.args.get("include_source_file", "0").lower() in {"1","true","yes"})

    if not domain:
        return jsonify({"error": "domain required"}), 400
    if mapping_schema_id is None:
        return jsonify({"error": "mapping_schema_id required"}), 400
    if fmt not in {"csv", "xlsx"}:
        return jsonify({"error": "fmt must be 'csv' or 'xlsx'"}), 400

    filename_base = f"SDTM_{domain.upper()}_schema{mapping_schema_id}" + (f"_sf{source_file_id}" if source_file_id else "")

    if fmt == "csv":
        gen = sdtm_download_service.stream_sdtm_csv(
            domain=domain,
            mapping_schema_id=mapping_schema_id,
            source_file_id=source_file_id,
            include_source_file=include_source_file,
        )
        headers = {
            "Content-Disposition": f'attachment; filename="{filename_base}.csv"',
            "Content-Type": "text/csv; charset=utf-8",
        }
        return Response(gen, headers=headers)

    # XLSX
    wb_bytes = sdtm_download_service.build_sdtm_xlsx(
        domain=domain,
        mapping_schema_id=mapping_schema_id,
        source_file_id=source_file_id,
        include_source_file=include_source_file,
        sheet_name=domain.upper(),
    )
    return send_file(
        wb_bytes,
        as_attachment=True,
        download_name=f"{filename_base}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
