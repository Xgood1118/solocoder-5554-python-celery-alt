import functools
import base64

from flask import request, jsonify
import config


def check_auth(username, password):
    return username == config.AUTH_USERNAME and password == config.AUTH_PASSWORD


def check_token(token):
    return token == config.AUTH_TOKEN


def authenticate():
    return jsonify({"error": "Authentication required"}), 401, {
        "WWW-Authenticate": 'Basic realm="Task Scheduler"'
    }


def require_auth(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get("Authorization", "")
        token_header = request.headers.get("X-Auth-Token", "")

        if token_header and check_token(token_header):
            return f(*args, **kwargs)

        if auth_header.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
                username, password = decoded.split(":", 1)
                if check_auth(username, password):
                    return f(*args, **kwargs)
            except Exception:
                pass

        if request.args.get("token") and check_token(request.args.get("token")):
            return f(*args, **kwargs)

        return authenticate()

    return decorated


def get_actor():
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Basic "):
        try:
            decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
            return decoded.split(":", 1)[0]
        except Exception:
            pass
    return "anonymous"


def get_client_ip():
    if request.headers.get("X-Forwarded-For"):
        return request.headers.get("X-Forwarded-For").split(",")[0].strip()
    return request.remote_addr or "127.0.0.1"
