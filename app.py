from gevent import monkey
monkey.patch_all()
from flask import Flask, request, jsonify
import psycopg2
import json
import time
import requests as http_requests
import psycopg2.errors
from psycopg2.extras import RealDictCursor
import firebase_admin
from firebase_admin import credentials, messaging
from flask_cors import CORS
from flask_socketio import SocketIO
import os
import math
import gevent
from collections import defaultdict
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
DEFAULT_CORS_ORIGINS = ",".join([
    "https://navneetbhaltilak.github.io",
    "http://localhost:5000",
    "http://localhost:5500",
    "http://127.0.0.1:5500",
])
CORS_ORIGINS = [
    origin.strip()
    for origin in os.environ.get("CORS_ORIGINS", DEFAULT_CORS_ORIGINS).split(",")
    if origin.strip()
]
CORS(app, resources={r"/api/*": {"origins": CORS_ORIGINS}})
socketio = SocketIO(app, cors_allowed_origins=CORS_ORIGINS)

STALE_EMERGENCY_SECONDS = max(30, int(os.environ.get("STALE_EMERGENCY_SECONDS", "120")))

@app.errorhandler(Exception)
def handle_unexpected_error(e):
    import traceback
    traceback.print_exc()
    code = getattr(e, "code", 500)
    return jsonify({"error": str(e)}), code if isinstance(code, int) else 500

firebase_creds_json = os.environ.get("FIREBASE_CREDENTIALS_JSON")
if firebase_creds_json:
    cred = credentials.Certificate(json.loads(firebase_creds_json))
else:
    cred = credentials.Certificate("firebase-service-account.json")
firebase_admin.initialize_app(cred)
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set")
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return psycopg2.connect(
                DATABASE_URL,
                cursor_factory=RealDictCursor
            )
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                print(f"DB connection attempt {attempt + 1} failed, retrying...")
                time.sleep(1)
            else:
                raise

def require_driver_password(handler):
    """Each ambulance now has its own password (set at registration) rather
    than every driver sharing one global key. The password travels in the
    JSON body alongside ambulance_id, since that's what the driver app
    already sends — no separate login/session/token machinery needed."""
    @wraps(handler)
    def wrapped(*args, **kwargs):
        data = request_json_object()
        if data is None:
            return jsonify({"error": "Request body must be JSON"}), 400
        ambulance_id = data.get("ambulance_id")
        password = data.get("password")
        if not ambulance_id or not password:
            return jsonify({"error": "ambulance_id and password are required"}), 401
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT password_hash FROM ambulances WHERE ambulance_id = %s", (ambulance_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row is None:
            return jsonify({"error": f"Ambulance '{ambulance_id}' is not registered."}), 404
        if not row["password_hash"]:
            return jsonify({"error": "This ambulance has no password set yet. Register it with a password first."}), 401
        if not check_password_hash(row["password_hash"], password):
            return jsonify({"error": "Incorrect password."}), 401
        return handler(*args, **kwargs)
    return wrapped

def request_json_object():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return None
    return data

def valid_coordinate(value, minimum, maximum):
    try:
        coordinate = float(value)
    except (TypeError, ValueError):
        return None
    return coordinate if minimum <= coordinate <= maximum else None

def require_fields(data, fields):
    missing = [f for f in fields if data.get(f) in (None, "")]
    if missing:
        return jsonify({"error": f"Missing required field(s): {', '.join(missing)}"}), 400
    return None

# Lightweight in-memory rate limiter — no extra dependency needed. Fine on
# a single worker process (which is what this app runs as); would need a
# shared store (e.g. Redis) if this ever moves to multiple workers/instances.
_rate_limit_last_seen = defaultdict(float)

def rate_limited(key, min_interval_seconds):
    now = time.time()
    if now - _rate_limit_last_seen[key] < min_interval_seconds:
        return True
    _rate_limit_last_seen[key] = now
    return False

def get_alert_level(distance_km):
    if distance_km <= 0.5:
        return "critical"
    elif distance_km <= 2:
        return "high"
    elif distance_km <= 5:
        return "info"
    return None

def get_last_alert_level(cur, event_id, user_id):
    cur.execute("""
        SELECT alert_level FROM notifications
        WHERE event_id = %s AND user_id = %s
        ORDER BY sent_time DESC LIMIT 1
    """, (event_id, user_id))
    row = cur.fetchone()
    return row["alert_level"] if row else None

def send_fcm_notification(token, ambulance_id, distance_km, level):
    titles = {
        "info": "Ambulance Approaching",
        "high": "Ambulance Nearby — Prepare to Give Way",
        "critical": "Ambulance Very Close — Give Way Now",
        "clear": "All Clear"
    }
    if level == "clear":
        body = "The ambulance has passed or the emergency has ended."
    else:
        body = f"Ambulance {ambulance_id} is {distance_km:.1f} km away."

    message = messaging.Message(
        data={
            "title": titles.get(level, "Ambulance Alert"),
            "body": body,
            "ambulance_id": ambulance_id,
            "distance": str(distance_km) if distance_km else "",
            "level": level
        },
        token=token,
    )
    messaging.send(message)
    return "sent"

def is_near_route(cur, event_id, lat, lon, threshold_meters=200):
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM emergency_events
            WHERE event_id = %s AND route_geom IS NOT NULL
            AND ST_DWithin(
                route_geom::geography,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s
            )
        ) AS near_route
    """, (event_id, lon, lat, threshold_meters))
    return cur.fetchone()["near_route"]

def calculate_bearing(lat1, lon1, lat2, lon2):
    lat1, lat2 = math.radians(lat1), math.radians(lat2)
    diff_lon = math.radians(lon2 - lon1)
    x = math.sin(diff_lon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(diff_lon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360

def bearing_difference(b1, b2):
    diff = abs(b1 - b2) % 360
    return min(diff, 360 - diff)

def get_users_needing_standdown(cur, event_id, current_nearby_user_ids):
    cur.execute("""
        SELECT DISTINCT n.user_id, u.fcm_token
        FROM notifications n
        JOIN users u ON u.user_id = n.user_id
        WHERE n.event_id = %s AND n.alert_level != 'clear'
    """, (event_id,))
    previously_alerted = cur.fetchall()

    standdown_list = []
    for row in previously_alerted:
        if row["user_id"] not in current_nearby_user_ids:
            standdown_list.append(row)
    return standdown_list

def get_all_alerted_users(cur, event_ids):
    if not event_ids:
        return []
    cur.execute("""
        SELECT DISTINCT n.user_id, u.fcm_token
        FROM notifications n
        JOIN users u ON u.user_id = n.user_id
        WHERE n.event_id = ANY(%s) AND n.alert_level != 'clear'
    """, (event_ids,))
    return cur.fetchall()

def is_near_road(cur, lat, lon, threshold_meters=30):
    cur.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM roads
            WHERE ST_DWithin(
                geom::geography,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s
            )
        ) AS near_road
    """, (lon, lat, threshold_meters))
    return cur.fetchone()["near_road"]

def geocode_address(address):
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": address, "format": "json", "limit": 1}
        headers = {"User-Agent": "EmergencyAmbulanceAlert/1.0"}
        resp = http_requests.get(url, params=params, headers=headers, timeout=5)
        results = resp.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        print(f"Geocoding failed: {e}")
    return None, None

def is_ahead(ambulance_bearing, bearing_to_user, cone_degrees=90):
    return bearing_difference(ambulance_bearing, bearing_to_user) <= cone_degrees

def end_emergency_and_notify(ambulance_id):
    """Shared by the manual /emergency/end endpoint and the stale-emergency
    watcher. Marks the emergency ended and sends an all-clear push to every
    user who was ever alerted for it — previously nobody was told an
    emergency had ended at all."""
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT event_id FROM emergency_events
        WHERE ambulance_id = %s AND status = 'active'
    """, (ambulance_id,))
    event_ids = [row["event_id"] for row in cur.fetchall()]

    cur.execute("UPDATE ambulances SET status = 'idle' WHERE ambulance_id = %s", (ambulance_id,))
    cur.execute("""
        UPDATE emergency_events SET status = 'ended', end_time = NOW()
        WHERE ambulance_id = %s AND status = 'active'
    """, (ambulance_id,))

    alerted_users = get_all_alerted_users(cur, event_ids)
    for user in alerted_users:
        try:
            status_str = send_fcm_notification(user["fcm_token"], ambulance_id, None, "clear")
        except Exception as e:
            print(f"All-clear FCM send failed: {e}")
            status_str = "failed"
        if event_ids:
            cur.execute("""
                INSERT INTO notifications (event_id, user_id, distance_km, alert_level, status)
                VALUES (%s, %s, %s, %s, %s)
            """, (event_ids[0], user["user_id"], None, "clear", status_str))

    socketio.emit("ambulance_ended", {"ambulance_id": ambulance_id})
    conn.commit()
    cur.close()
    conn.close()
    return len(alerted_users)

def stale_emergency_watcher():
    """Runs for the lifetime of the process. An ambulance stuck in
    'emergency' with no location update in a while (dropped connection,
    crashed app, driver forgot to tap End) used to stay live forever,
    misleading everyone still watching it on the dashboard or getting
    alerts. This ends it automatically and sends the same all-clear."""
    while True:
        gevent.sleep(30)
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("""
                SELECT ambulance_id FROM ambulances
                WHERE status = 'emergency'
                  AND last_updated < NOW() - make_interval(secs => %s)
            """, (STALE_EMERGENCY_SECONDS,))
            stale = [row["ambulance_id"] for row in cur.fetchall()]
            cur.close()
            conn.close()
            for ambulance_id in stale:
                print(f"Auto-ending stale emergency for {ambulance_id} (no update in {STALE_EMERGENCY_SECONDS}s)")
                end_emergency_and_notify(ambulance_id)
        except Exception as e:
            print(f"Stale emergency watcher error: {e}")

gevent.spawn(stale_emergency_watcher)

@app.route("/api/users/register", methods=["POST"])
def register_user():
    data = request_json_object()
    if data is None:
        return jsonify({"error": "Request body must be JSON"}), 400
    err = require_fields(data, ["name", "phone", "latitude", "longitude", "fcm_token"])
    if err: return err
    lat = valid_coordinate(data["latitude"], -90, 90)
    lng = valid_coordinate(data["longitude"], -180, 180)
    if lat is None or lng is None:
        return jsonify({"error": "latitude/longitude out of range or not numeric"}), 400
    name = data["name"]
    phone = data["phone"]
    fcm_token = data["fcm_token"]

    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO users (name, phone, location, fcm_token)
        VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s)
        RETURNING user_id
    """, (name, phone, lng, lat, fcm_token))
    user_id = cur.fetchone()["user_id"]
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"user_id": user_id}), 201

@app.route("/api/users/location", methods=["PUT"])
def update_user_location():
    data = request_json_object()
    if data is None:
        return jsonify({"error": "Request body must be JSON"}), 400
    err = require_fields(data, ["user_id", "latitude", "longitude"])
    if err: return err
    lat = valid_coordinate(data["latitude"], -90, 90)
    lng = valid_coordinate(data["longitude"], -180, 180)
    if lat is None or lng is None:
        return jsonify({"error": "latitude/longitude out of range or not numeric"}), 400
    user_id = data["user_id"]

    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        UPDATE users
        SET location = ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
            last_updated = NOW()
        WHERE user_id = %s
    """, (lng, lat, user_id))
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"status": "updated"}), 200

@app.route("/api/ambulance/register", methods=["POST"])
def register_ambulance():
    if rate_limited(f"register:{request.remote_addr}", 5):
        return jsonify({"error": "Too many requests, slow down."}), 429
    data = request_json_object()
    if data is None:
        return jsonify({"error": "Request body must be JSON"}), 400
    err = require_fields(data, ["ambulance_id", "vehicle_number", "latitude", "longitude", "password"])
    if err: return err
    lat = valid_coordinate(data["latitude"], -90, 90)
    lng = valid_coordinate(data["longitude"], -180, 180)
    if lat is None or lng is None:
        return jsonify({"error": "latitude/longitude out of range or not numeric"}), 400
    ambulance_id = data["ambulance_id"]
    vehicle_number = data["vehicle_number"]
    password = data["password"]
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters."}), 400
    password_hash = generate_password_hash(password)

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO ambulances (ambulance_id, vehicle_number, location, status, password_hash)
            VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, 'idle', %s)
        """, (ambulance_id, vehicle_number, lng, lat, password_hash))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"status": "registered"}), 201

    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        # Ambulance ID already exists. If it predates password auth (no
        # password_hash yet), let this call set one instead of hard-failing
        # — otherwise every ambulance registered before this migration
        # would be permanently locked out with no way back in.
        cur.execute("SELECT password_hash FROM ambulances WHERE ambulance_id = %s", (ambulance_id,))
        existing = cur.fetchone()
        if existing and not existing["password_hash"]:
            cur.execute("UPDATE ambulances SET password_hash = %s WHERE ambulance_id = %s", (password_hash, ambulance_id))
            conn.commit()
            cur.close()
            conn.close()
            return jsonify({"status": "password_set"}), 200
        cur.close()
        conn.close()
        return jsonify({"error": f"Ambulance with ID '{ambulance_id}' is already registered with a password. If you forgot it, this needs a manual reset."}), 409

@app.route("/api/ambulance/emergency/start", methods=["POST"])
@require_driver_password
def start_emergency():
    if rate_limited(f"start:{request.remote_addr}", 2):
        return jsonify({"error": "Too many requests, slow down."}), 429
    data = request_json_object()
    if data is None:
        return jsonify({"error": "Request body must be JSON"}), 400
    err = require_fields(data, ["ambulance_id", "destination"])
    if err: return err
    ambulance_id = data["ambulance_id"]
    destination = data.get("destination", None)
    dest_lat = data.get("destination_lat")
    dest_lng = data.get("destination_lng")

    if not destination or not destination.strip():
        return jsonify({"error": "destination is required"}), 400

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT ST_Y(location::geometry) AS lat, ST_X(location::geometry) AS lng
        FROM ambulances WHERE ambulance_id = %s
    """, (ambulance_id,))
    amb_loc = cur.fetchone()

    if amb_loc is None:
        cur.close()
        conn.close()
        return jsonify({"error": f"Ambulance '{ambulance_id}' is not registered. Register it first."}), 404

    # Idempotent start: if this ambulance already has an active emergency
    # (double-tapped Start, retried after a network blip), resume the
    # existing one instead of creating a duplicate event.
    cur.execute("""
        SELECT event_id, destination_lat, destination_lng, route_geojson
        FROM emergency_events WHERE ambulance_id = %s AND status = 'active'
        ORDER BY start_time DESC LIMIT 1
    """, (ambulance_id,))
    existing_event = cur.fetchone()
    if existing_event:
        cur.execute("SELECT destination FROM ambulances WHERE ambulance_id = %s", (ambulance_id,))
        existing_destination = cur.fetchone()["destination"]
        cur.close()
        conn.close()
        existing_route = existing_event["route_geojson"]
        if isinstance(existing_route, str):
            try:
                existing_route = json.loads(existing_route)
            except Exception:
                pass
        return jsonify({
            "event_id": existing_event["event_id"],
            "destination": existing_destination,
            "destination_lat": existing_event["destination_lat"],
            "destination_lng": existing_event["destination_lng"],
            "has_route": existing_route is not None,
            "route_geojson": existing_route,
            "resumed": True
        }), 200

    if destination and not (dest_lat and dest_lng):
        dest_lat, dest_lng = geocode_address(destination)

    route_geojson = None
    if dest_lat and dest_lng and amb_loc:
        try:
            osrm_url = f"http://router.project-osrm.org/route/v1/driving/{amb_loc['lng']},{amb_loc['lat']};{dest_lng},{dest_lat}?overview=full&geometries=geojson"
            resp = http_requests.get(osrm_url, timeout=5)
            route_data = resp.json()
            if route_data.get("code") == "Ok":
                route_geojson = route_data["routes"][0]["geometry"]
        except Exception as e:
            print(f"Routing failed: {e}")

    cur.execute("""
        UPDATE ambulances SET status = 'emergency', destination = %s WHERE ambulance_id = %s
    """, (destination, ambulance_id))

    route_geom_sql = None
    if route_geojson:
        coords = route_geojson["coordinates"]
        linestring_wkt = "LINESTRING(" + ", ".join(f"{c[0]} {c[1]}" for c in coords) + ")"
        route_geom_sql = linestring_wkt

    cur.execute("""
        INSERT INTO emergency_events (ambulance_id, status, destination_lat, destination_lng, route_geojson, route_geom)
        VALUES (%s, 'active', %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4326))
        RETURNING event_id
    """, (ambulance_id, dest_lat, dest_lng, json.dumps(route_geojson) if route_geojson else None, route_geom_sql))
    event_id = cur.fetchone()["event_id"]

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({
        "event_id": event_id,
        "destination": destination,
        "destination_lat": dest_lat,
        "destination_lng": dest_lng,
        "has_route": route_geojson is not None,
        "route_geojson": route_geojson
    }), 201

@app.route("/api/ambulance/location", methods=["POST"])
@require_driver_password
def ambulance_location_ping():
    data = request_json_object()
    if data is None:
        return jsonify({"error": "Request body must be JSON"}), 400
    if rate_limited(f"loc:{data.get('ambulance_id', request.remote_addr)}", 2):
        return jsonify({"error": "Too many requests, slow down."}), 429
    err = require_fields(data, ["ambulance_id", "latitude", "longitude"])
    if err: return err
    lat = valid_coordinate(data["latitude"], -90, 90)
    lng = valid_coordinate(data["longitude"], -180, 180)
    if lat is None or lng is None:
        return jsonify({"error": "latitude/longitude out of range or not numeric"}), 400
    ambulance_id = data["ambulance_id"]
    speed = data.get("speed", 0)

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        UPDATE ambulances
        SET location = ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
            speed = %s, last_updated = NOW()
        WHERE ambulance_id = %s
        RETURNING status, destination
    """, (lng, lat, speed, ambulance_id))
    amb_row = cur.fetchone()
    if amb_row is None:
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"error": f"Ambulance '{ambulance_id}' is not registered."}), 404
    status = amb_row["status"]
    destination = amb_row["destination"]

    if status != "emergency":
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"status": "location updated, not in emergency"}), 200

    cur.execute("""
        SELECT event_id FROM emergency_events
        WHERE ambulance_id = %s AND status = 'active'
        ORDER BY start_time DESC LIMIT 1
    """, (ambulance_id,))
    event_row = cur.fetchone()
    if event_row is None:
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"error": "Ambulance is marked emergency but has no active event", "notified": 0}), 409
    event_id = event_row["event_id"]
    cur.execute("SELECT prev_latitude, prev_longitude FROM ambulances WHERE ambulance_id = %s", (ambulance_id,))
    prev = cur.fetchone()

    ambulance_bearing = None
    if prev["prev_latitude"] is not None:
        ambulance_bearing = calculate_bearing(prev["prev_latitude"], prev["prev_longitude"], lat, lng)

    cur.execute("""
        SELECT u.user_id, u.fcm_token,
            ST_Distance(u.location, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) / 1000 AS distance_km,
            ST_Y(u.location::geometry) AS latitude,
            ST_X(u.location::geometry) AS longitude
        FROM users u
        WHERE u.alert_enabled = TRUE
            AND ST_DWithin(u.location, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, 5000)
    """, (lng, lat, lng, lat))
    nearby_users = cur.fetchall()

    notified_count = 0
    cur.execute("SELECT route_geom IS NOT NULL AS has_route FROM emergency_events WHERE event_id = %s", (event_id,))
    has_route = cur.fetchone()["has_route"]

    for user in nearby_users:
        passes = has_route and is_near_route(cur, event_id, user["latitude"], user["longitude"], threshold_meters=500)
        if not passes:
            # No route, or the route check missed them (GPS drift, route
            # snapping, being on a parallel street) — give them a second
            # chance via direction + road proximity instead of silently
            # excluding a genuinely nearby person. Missing a real alert is
            # worse than sending an extra one.
            bearing_ok = True
            if ambulance_bearing is not None:
                bearing_to_user = calculate_bearing(lat, lng, user["latitude"], user["longitude"])
                bearing_ok = is_ahead(ambulance_bearing, bearing_to_user, cone_degrees=120)
            near_road = is_near_road(cur, user["latitude"], user["longitude"], threshold_meters=100)
            passes = bearing_ok and near_road
        if not passes:
            continue
        new_level = get_alert_level(user["distance_km"])
        if new_level is None:
            continue

        last_level = get_last_alert_level(cur, event_id, user["user_id"])
        if new_level == last_level:
            continue

        try:
            status_str = send_fcm_notification(user["fcm_token"], ambulance_id, user["distance_km"], new_level)
        except Exception as e:
            print(f"FCM send failed: {e}")
            status_str = "failed"
        cur.execute("""
            INSERT INTO notifications (event_id, user_id, distance_km, alert_level, status)
            VALUES (%s, %s, %s, %s, %s)
        """, (event_id, user["user_id"], user["distance_km"], new_level, status_str))
        if status_str == "sent":
            notified_count += 1

    current_nearby_ids = {u["user_id"] for u in nearby_users}
    standdown_users = get_users_needing_standdown(cur, event_id, current_nearby_ids)

    for user in standdown_users:
        try:
            status_str = send_fcm_notification(user["fcm_token"], ambulance_id, None, "clear")
        except Exception as e:
            print(f"FCM send failed: {e}")
            status_str = "failed"
        cur.execute("""
            INSERT INTO notifications (event_id, user_id, distance_km, alert_level, status)
            VALUES (%s, %s, %s, %s, %s)
        """, (event_id, user["user_id"], None, "clear", status_str))

    cur.execute("""
        UPDATE ambulances SET prev_latitude = %s, prev_longitude = %s WHERE ambulance_id = %s
    """, (lat, lng, ambulance_id))
    socketio.emit('ambulance_update', {
        "ambulance_id": ambulance_id,
        "latitude": lat,
        "longitude": lng,
        "speed": speed,
        "alerted_count": notified_count,
        "destination": destination
    })
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"notified": notified_count}), 200

@app.route("/api/ambulance/status/<ambulance_id>", methods=["GET"])
def get_ambulance_status(ambulance_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT status, destination FROM ambulances WHERE ambulance_id = %s", (ambulance_id,))
    row = cur.fetchone()

    if row is None:
        cur.close()
        conn.close()
        return jsonify({"status": "not_found"}), 404

    result = {"status": row["status"]}

    if row["status"] == "emergency":
        result["destination"] = row["destination"]
        cur.execute("""
            SELECT destination_lat, destination_lng, route_geojson
            FROM emergency_events
            WHERE ambulance_id = %s AND status = 'active'
            ORDER BY start_time DESC LIMIT 1
        """, (ambulance_id,))
        event = cur.fetchone()
        if event:
            route_geojson = event["route_geojson"]
            if isinstance(route_geojson, str):
                try:
                    route_geojson = json.loads(route_geojson)
                except Exception:
                    pass
            result["destination_lat"] = event["destination_lat"]
            result["destination_lng"] = event["destination_lng"]
            result["route_geojson"] = route_geojson

    cur.close()
    conn.close()
    return jsonify(result), 200

@app.route("/api/ambulance/emergency/active/<ambulance_id>", methods=["GET"])
def get_active_emergency(ambulance_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT status, destination, ST_Y(location::geometry) AS latitude, ST_X(location::geometry) AS longitude
        FROM ambulances WHERE ambulance_id = %s
    """, (ambulance_id,))
    amb = cur.fetchone()

    if amb is None:
        cur.close()
        conn.close()
        return jsonify({"status": "not_found"}), 404

    if amb["status"] != "emergency":
        cur.close()
        conn.close()
        return jsonify({"status": amb["status"]}), 200

    cur.execute("""
        SELECT event_id, destination_lat, destination_lng, route_geojson
        FROM emergency_events
        WHERE ambulance_id = %s AND status = 'active'
        ORDER BY start_time DESC LIMIT 1
    """, (ambulance_id,))
    event = cur.fetchone()
    cur.close()
    conn.close()

    if event is None:
        return jsonify({"status": "emergency", "event": None, "destination": amb["destination"], "latitude": amb["latitude"], "longitude": amb["longitude"]}), 200

    return jsonify({
        "status": "emergency",
        "event_id": event["event_id"],
        "destination": amb["destination"],
        "destination_lat": event["destination_lat"],
        "destination_lng": event["destination_lng"],
        "route_geojson": event["route_geojson"],
        "latitude": amb["latitude"],
        "longitude": amb["longitude"]
    }), 200

@app.route("/api/ambulance/emergency/end", methods=["POST"])
@require_driver_password
def end_emergency():
    data = request_json_object()
    if data is None:
        return jsonify({"error": "Request body must be JSON"}), 400
    err = require_fields(data, ["ambulance_id"])
    if err: return err
    ambulance_id = data["ambulance_id"]

    notified = end_emergency_and_notify(ambulance_id)
    return jsonify({"status": "emergency ended", "all_clear_sent_to": notified}), 200

@app.route("/api/users/update-token", methods=["POST"])
def update_token():
    data = request_json_object()
    if data is None:
        return jsonify({"error": "Request body must be JSON"}), 400
    err = require_fields(data, ["user_id", "fcm_token"])
    if err: return err
    user_id = data["user_id"]
    fcm_token = data["fcm_token"]

    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE users SET fcm_token = %s WHERE user_id = %s", (fcm_token, user_id))
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"status": "token updated"}), 200

@app.route("/api/users/register-device", methods=["POST"])
def register_device():
    data = request_json_object()
    if data is None:
        return jsonify({"error": "Request body must be JSON"}), 400
    err = require_fields(data, ["device_id"])
    if err: return err
    device_id = data["device_id"]
    lat = data.get("latitude")
    lng = data.get("longitude")

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT user_id FROM users WHERE device_id = %s", (device_id,))
    existing = cur.fetchone()

    if existing:
        user_id = existing["user_id"]
    else:
        cur.execute("""
            INSERT INTO users (device_id, name, location, alert_enabled)
            VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, TRUE)
            RETURNING user_id
        """, (device_id, f"Device-{device_id[:8]}", lng or 0, lat or 0))
        user_id = cur.fetchone()["user_id"]

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"user_id": user_id}), 200

@app.route("/api/health", methods=["GET"])
def health_check():
    # No DB call on purpose — this exists purely for an external uptime
    # pinger to keep the free-tier instance from sleeping, so it shouldn't
    # add any load or dependency risk of its own.
    return jsonify({"status": "ok"}), 200

@app.route("/api/dashboard/active", methods=["GET"])
def dashboard_active():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT ambulance_id, vehicle_number, speed, destination,
               ST_Y(location::geometry) AS latitude,
               ST_X(location::geometry) AS longitude
        FROM ambulances
        WHERE status = 'emergency'
    """)
    active = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify(active), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port, debug=False)