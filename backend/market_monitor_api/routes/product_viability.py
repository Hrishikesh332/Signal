from flask import Blueprint, current_app, jsonify, request

from market_monitor_api.services.product_viability import (
    ProductViabilityError,
    build_product_viability_response,
    parse_product_viability_input,
)


product_viability_bp = Blueprint("product_viability", __name__)


@product_viability_bp.post("/product-viability")
def post_product_viability():
    try:
        if request.mimetype != "multipart/form-data":
            raise ProductViabilityError(
                "product_viability_invalid_content_type",
                "Submit product viability requests as multipart/form-data.",
                400,
            )
        settings = current_app.config["SETTINGS"]
        payload = parse_product_viability_input(request.form, request.files, settings)
        return jsonify(build_product_viability_response(settings, payload)), 200
    except ProductViabilityError as exc:
        return jsonify({"error": {"code": exc.code, "message": exc.message}}), exc.status_code
    except Exception:
        return (
            jsonify(
                {
                    "error": {
                        "code": "product_viability_unavailable",
                        "message": "Unable to analyze product viability.",
                    }
                }
            ),
            500,
        )
