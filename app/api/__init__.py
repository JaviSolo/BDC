from flask import Blueprint, jsonify

api_bp = Blueprint("api", __name__)

@api_bp.route("/status", methods=["GET"])
def status():
    return jsonify({"status": "API is running", "message": "Welcome to the Basketball Data Center"}), 200
