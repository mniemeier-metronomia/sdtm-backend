from flask import Flask, jsonify, request
from flask_cors import CORS


app = Flask(__name__)
CORS(app)

from vault.vault_routes import vault_bp
app.register_blueprint(vault_bp)

from source_files.source_files_routes import source_files_bp
app.register_blueprint(source_files_bp)

from project.project_routes import project_bp
app.register_blueprint(project_bp)

from sdtm.sdtm_routes import sdtm_bp
app.register_blueprint(sdtm_bp)

from mapper.mapper_routes import mapper_bp
app.register_blueprint(mapper_bp)









if __name__ == '__main__':
    app.run(debug=True, port=5000)