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

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")
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

    # Data-only message: no top-level "notification" field. If we include
    # one, Firebase auto-displays a system popup with the OS default sound
    # regardless of whether the app is open — which was firing alongside
    # our own in-app alert and showing up as duplicated notifications.
    # With data-only, the app (foreground JS or the service worker in the
    # background) decides if/how to display it.
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

@app.route("/api/users/register", methods=["POST"])
def register_user():
    data = request.json
    name = data["name"]
    phone = data["phone"]
    lat = data["latitude"]
    lng = data["longitude"]
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
    data = request.json
    user_id = data["user_id"]
    lat = data["latitude"]
    lng = data["longitude"]

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
    data = request.json
    ambulance_id = data["ambulance_id"]
    vehicle_number = data["vehicle_number"]
    lat = data["latitude"]
    lng = data["longitude"]

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO ambulances (ambulance_id, vehicle_number, location, status)
            VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, 'idle')
        """, (ambulance_id, vehicle_number, lng, lat))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"status": "registered"}), 201

    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        cur.close()
        conn.close()
        return jsonify({"error": f"Ambulance with ID '{ambulance_id}' is already registered."}), 409
@app.route("/api/ambulance/emergency/start", methods=["POST"])
def start_emergency():
    data = request.json
    ambulance_id = data["ambulance_id"]
    destination = data.get("destination", None)
    dest_lat = data.get("destination_lat")
    dest_lng = data.get("destination_lng")

    if not destination or not destination.strip():
        return jsonify({"error": "destination is required"}), 400

    # If a destination name was given but no coordinates, geocode it
    if destination and not (dest_lat and dest_lng):
        dest_lat, dest_lng = geocode_address(destination)

    conn = get_db()
    cur = conn.cursor()

    # Get ambulance's current location to compute route FROM
    cur.execute("""
        SELECT ST_Y(location::geometry) AS lat, ST_X(location::geometry) AS lng
        FROM ambulances WHERE ambulance_id = %s
    """, (ambulance_id,))
    amb_loc = cur.fetchone()

    route_geojson = None
    if dest_lat and dest_lng and amb_loc:
        try:
            osrm_url = f"http://router.project-osrm.org/route/v1/driving/{amb_loc['lng']},{amb_loc['lat']};{dest_lng},{dest_lat}?overview=full&geometries=geojson"
            resp = http_requests.get(osrm_url, timeout=5)
            route_data = resp.json()
            if route_data.get("code") == "Ok":
                route_geojson = route_data["routes"][0]["geometry"]  # a GeoJSON LineString
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
def ambulance_location_ping():
    data = request.json
    ambulance_id = data["ambulance_id"]
    lat = data["latitude"]
    lng = data["longitude"]
    speed = data.get("speed", 0)

    conn = get_db()
    cur = conn.cursor()

    # 1. Update ambulance location
    cur.execute("""
        UPDATE ambulances
        SET location = ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
            speed = %s, last_updated = NOW()
        WHERE ambulance_id = %s
        RETURNING status
    """, (lng, lat, speed, ambulance_id))
    status = cur.fetchone()["status"]

    if status != "emergency":
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"status": "location updated, not in emergency"}), 200

    # 2. Find the active event for this ambulance
    cur.execute("""
        SELECT event_id FROM emergency_events
        WHERE ambulance_id = %s AND status = 'active'
        ORDER BY start_time DESC LIMIT 1
    """, (ambulance_id,))
    event_id = cur.fetchone()["event_id"]
    cur.execute("SELECT prev_latitude, prev_longitude FROM ambulances WHERE ambulance_id = %s", (ambulance_id,))
    prev = cur.fetchone()

    ambulance_bearing = None
    if prev["prev_latitude"] is not None:
        ambulance_bearing = calculate_bearing(prev["prev_latitude"], prev["prev_longitude"], lat, lng)

    # 3. Find users within 5km who haven't already been notified for this event
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

    # 4. Send notifications and log them
    notified_count = 0
    cur.execute("SELECT route_geom IS NOT NULL AS has_route FROM emergency_events WHERE event_id = %s", (event_id,))
    has_route = cur.fetchone()["has_route"]

    for user in nearby_users:
        if has_route:
            # Route-based filtering: is this user actually near the planned path?
            if not is_near_route(cur, event_id, user["latitude"], user["longitude"]):
                continue
        else:
            # Fallback: direction + road-based filtering (no destination set)
            if ambulance_bearing is not None:
                bearing_to_user = calculate_bearing(lat, lng, user["latitude"], user["longitude"])
                if not is_ahead(ambulance_bearing, bearing_to_user):
                    continue
            near_road = is_near_road(cur, user["latitude"], user["longitude"])
            if not near_road:
                continue
        new_level = get_alert_level(user["distance_km"])
        if new_level is None:
            continue

        last_level = get_last_alert_level(cur, event_id, user["user_id"])
        if new_level == last_level:
            continue  # same zone as before — skip, avoid spam

        try:
            status_str = send_fcm_notification(user["fcm_token"], ambulance_id, user["distance_km"], new_level)
        except Exception as e:
            print(f"FCM send failed: {e}")
            status_str = "failed"
        cur.execute("""
            INSERT INTO notifications (event_id, user_id, distance_km, alert_level, status)
            VALUES (%s, %s, %s, %s, %s)
        """, (event_id, user["user_id"], user["distance_km"], new_level, status_str))
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
        "alerted_count": notified_count
    })
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"notified": notified_count}), 200
@app.route("/api/ambulance/status/<ambulance_id>", methods=["GET"])
def get_ambulance_status(ambulance_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT status FROM ambulances WHERE ambulance_id = %s", (ambulance_id,))
    row = cur.fetchone()

    if row is None:
        cur.close()
        conn.close()
        return jsonify({"status": "not_found"}), 404

    result = {"status": row["status"]}

    if row["status"] == "emergency":
        cur.execute("""
            SELECT destination, destination_lat, destination_lng, route_geojson
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
            result["destination"] = event["destination"]
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
        SELECT status, ST_Y(location::geometry) AS latitude, ST_X(location::geometry) AS longitude
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
        SELECT event_id, destination, destination_lat, destination_lng, route_geojson
        FROM emergency_events
        WHERE ambulance_id = %s AND status = 'active'
        ORDER BY start_time DESC LIMIT 1
    """, (ambulance_id,))
    event = cur.fetchone()
    cur.close()
    conn.close()

    if event is None:
        return jsonify({"status": "emergency", "event": None, "latitude": amb["latitude"], "longitude": amb["longitude"]}), 200

    return jsonify({
        "status": "emergency",
        "event_id": event["event_id"],
        "destination": event["destination"],
        "destination_lat": event["destination_lat"],
        "destination_lng": event["destination_lng"],
        "route_geojson": event["route_geojson"],
        "latitude": amb["latitude"],
        "longitude": amb["longitude"]
    }), 200

@app.route("/api/ambulance/emergency/end", methods=["POST"])
def end_emergency():
    data = request.json
    ambulance_id = data["ambulance_id"]

    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE ambulances SET status = 'idle' WHERE ambulance_id = %s", (ambulance_id,))
    cur.execute("""
        UPDATE emergency_events SET status = 'ended', end_time = NOW()
        WHERE ambulance_id = %s AND status = 'active'
    """, (ambulance_id,))
    socketio.emit("ambulance_ended", {"ambulance_id": ambulance_id})
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"status": "emergency ended"}), 200
@app.route("/api/users/update-token", methods=["POST"])
def update_token():
    data = request.json
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
    data = request.json
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

@app.route("/api/dashboard/active", methods=["GET"])
def dashboard_active():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT ambulance_id, vehicle_number, speed,
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