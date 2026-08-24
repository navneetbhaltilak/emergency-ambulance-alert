# Smart Ambulance 5KM Emergency Alert System

A real-time geospatial alert system that notifies nearby road users when an emergency ambulance is approaching, helping them clear the way faster and safely.

## What it does

When an ambulance driver starts an emergency, the system continuously tracks its live GPS location and identifies road users who are genuinely in its path — not just anyone within a radius. It sends tiered push notifications (info → high → critical) as the ambulance gets closer, and automatically clears the alert once the ambulance has passed or the emergency ends.

## Key engineering features

- **PostGIS-powered geospatial queries** — real distance and proximity calculations using `ST_DWithin` and `ST_Distance`, not naive coordinate math
- **Route-aware alerting** — integrates with OSRM (OpenStreetMap routing) to compute the ambulance's actual road path to its destination, and only alerts users genuinely near that path
- **Direction and road filtering** — bearing calculations ensure only users ahead of the ambulance's direction of travel are alerted, and road-proximity matching filters out people who are indoors or off-road
- **Debounced, zone-transition alerts** — users are notified once per zone change, not spammed on every GPS ping
- **Real push notifications** — Firebase Cloud Messaging (Admin SDK, OAuth2) delivers actual device notifications, with automatic token refresh handling
- **Live dashboard** — a Leaflet.js map with real-time updates via SocketIO, showing ambulance position, alert zones, and live stats
- **Driver and citizen web apps** — real GPS-based tracking (`watchPosition()`) for ambulance drivers, and an automatic, low-friction alert experience for citizens

## Tech stack

Flask · PostgreSQL + PostGIS · Supabase · Flask-SocketIO · Firebase Admin SDK · OSRM · Leaflet.js

## Status

Built as a final-year engineering project, fully functional end-to-end with real GPS devices, real push notifications, and live multi-ambulance tracking.
