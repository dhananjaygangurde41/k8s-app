# =========================
# Imports
# =========================
from flask import Flask, request, jsonify, render_template
import psycopg2
import redis
import os
from time import perf_counter

# OpenTelemetry
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor


# =========================
# OpenTelemetry Setup
# =========================
resource = Resource(attributes={
    "service.name": "flask-user-service"
})

trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)

otlp_exporter = OTLPSpanExporter(
    endpoint=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317"),
    insecure=True
)

trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(otlp_exporter)
)


# =========================
# Flask App
# =========================
app = Flask(__name__)

# ✅ Enable Auto Instrumentation (IMPORTANT)
FlaskInstrumentor().instrument_app(app)
RedisInstrumentor().instrument()
Psycopg2Instrumentor().instrument()


# =========================
# Redis & DB Connections
# =========================
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=6379,
    decode_responses=True
)

def get_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        database="usersdb",
        user="postgres",
        password="postgres"
    )


# =========================
# Routes
# =========================
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/add_user", methods=["POST"])
def add_user():
    user_id = request.form["id"]
    name = request.form["name"]

    with tracer.start_as_current_span("add-user"):
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
            (user_id, name)
        )
        conn.commit()
        cur.close()
        conn.close()

        redis_client.set(user_id, name)

    return "User added successfully"


@app.route("/get_user/<user_id>")
def get_user(user_id):

    start_time = perf_counter()

    with tracer.start_as_current_span("get-user-request"):

        # 🔹 Redis lookup span
        with tracer.start_as_current_span("redis-cache-lookup") as span:
            span.set_attribute("cache.key", user_id)
            name = redis_client.get(user_id)

        if name:
            return jsonify({
                "id": user_id,
                "name": name,
                "source": "redis",
                "time_ms": round((perf_counter() - start_time) * 1000, 2)
            })

        # 🔹 Database lookup span
        with tracer.start_as_current_span("db-lookup") as span:
            span.set_attribute("db.system", "postgresql")
            span.set_attribute("db.table", "users")

            conn = get_db()
            cur = conn.cursor()
            cur.execute("SELECT name FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            cur.close()
            conn.close()

        if row:
            redis_client.set(user_id, row[0])
            return jsonify({
                "id": user_id,
                "name": row[0],
                "source": "database",
                "time_ms": round((perf_counter() - start_time) * 1000, 2)
            })

        return jsonify({"error": "User not found"}), 404


# =========================
# App Start
# =========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
