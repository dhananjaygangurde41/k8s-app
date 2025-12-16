from flask import Flask, request, jsonify, render_template
import psycopg2
import redis
import os

app = Flask(__name__)

# Redis
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=6379,
    decode_responses=True
)

# Postgres
def get_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        database="usersdb",
        user="postgres",
        password="postgres"
    )

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/add_user", methods=["POST"])
def add_user():
    user_id = request.form["id"]
    name = request.form["name"]

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
    # Check Redis
    name = redis_client.get(user_id)
    if name:
        return jsonify({
            "id": user_id,
            "name": name,
            "source": "redis"
        })

    # Check DB
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
            "source": "database"
        })

    return jsonify({"error": "User not found"}), 404


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
