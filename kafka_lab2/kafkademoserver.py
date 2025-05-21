import os
import random
import sqlite3
import sys
import uuid
import requests
import subprocess

from flask_socketio import SocketIO, emit
from flask import Flask, redirect, render_template_string, request, send_from_directory, jsonify

IMAGES_DIR = "images"
MAIN_DB = "main.db"

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

def get_db_connection():
    conn = sqlite3.connect(MAIN_DB)
    conn.row_factory = sqlite3.Row
    return conn

con = get_db_connection()
con.execute("CREATE TABLE IF NOT EXISTS image(id TEXT, filename TEXT, object TEXT)")
con.commit()
con.close()

if not os.path.exists(IMAGES_DIR):
    os.mkdir(IMAGES_DIR)

@app.route('/', methods=['GET'])
def index():
    con = get_db_connection()
    res = con.execute("SELECT * FROM image")
    images = res.fetchall()
    con.close()
    return render_template_string("""
<!DOCTYPE html>
<html>
<head>
  <script src="https://cdn.socket.io/4.0.0/socket.io.min.js"></script>
  <script>
    const socket = io();
    socket.on("update", (data) => {
      location.reload();
    });
    socket.on("message_processed", (data) => {
      location.reload();
    });
  </script>
</head>
<body>
  <form method="post" enctype="multipart/form-data">
    <input type="file" name="file" accept="image/*" />
    <button>Upload</button>
  </form>
   <div style="display: flex; flex-wrap: wrap; gap: 20px;">
   {% for image in images %}
      <div style="text-align: center;">
         <img src="/images/{{ image.filename }}" width="200" />
         <p>{{ image.object or 'undefined' }}</p>
      </div>
   {% endfor %}
   </div>

</body>
</html>
""", images=images)

@app.route('/images/<path:path>', methods=['GET'])
def image(path):
    return send_from_directory(IMAGES_DIR, path)

@app.route('/object/<id>', methods=['PUT'])
def set_object(id):
    con = get_db_connection()
    json_data = request.json
    label = json_data['object']
    con.execute("UPDATE image SET object = ? WHERE id = ?", (label, id))
    con.commit()
    con.close()

    # Emit WebSocket event
    socketio.emit('update', {'id': id, 'label': label})

    return {"status": "OK"}

@app.route('/image_meta/<id>', methods=['GET'])
def get_image_meta(id):
    con = get_db_connection()
    res = con.execute("SELECT filename FROM image WHERE id = ?", (id,))
    row = res.fetchone()
    con.close()
    if row:
        return {"filename": row["filename"]}
    return {"error": "not found"}, 404

@app.route('/bw/<id>', methods=['POST'])
def set_bw_image(id):
    json_data = request.json
    path = json_data["path"]

    con = get_db_connection()
    con.execute("UPDATE image SET filename = ? WHERE id = ?", (path, id))
    con.commit()
    con.close()

    socketio.emit("message_processed", {"id": id})
    return {"status": "OK"}

@app.route('/', methods=['POST'])
def upload_file():
    f = request.files['file']
    ext = f.filename.split('.')[-1]
    id = uuid.uuid4().hex
    filename = f"{id}.{ext}"
    f.save(os.path.join(IMAGES_DIR, filename))

    con = get_db_connection()
    con.execute("INSERT INTO image (id, filename, object) VALUES (?, ?, '')", (id, filename))
    con.commit()
    con.close()

    subprocess.Popen(["python3", "producer.py", id])

    return redirect('/')

if __name__ == '__main__':
    socketio.run(app, debug=True, port=5000)
