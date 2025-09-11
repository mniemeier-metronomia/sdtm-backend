from flask import Blueprint, request, jsonify
from vault.vault_service import VaultService
from werkzeug.utils import secure_filename

vault_bp = Blueprint("vault_bp", __name__)
vault_service = VaultService()


@vault_bp.route("/projects/<int:project_id>/upload", methods=["POST"])
def upload_project_file(project_id):
    file = request.files.get("file")

    if not file:
        return jsonify({"error": "Missing file"}), 400

    name = secure_filename(file.filename) or "unnamed"

    try:
        result = vault_service.upload_file(project_id, file, name)
        return jsonify(result), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@vault_bp.route("/files/<int:file_id>", methods=["DELETE"])
def delete_file(file_id):
    try:
        success = vault_service.delete_file(file_id)
        if not success:
            return jsonify({"error": "File not found"}), 404
        return jsonify({"message": "File deleted"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
