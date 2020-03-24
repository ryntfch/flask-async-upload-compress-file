from flask import Flask, request, jsonify, send_from_directory
from werkzeug.utils import secure_filename
from make_celery import make_celery
from subprocess import Popen, PIPE
from functools import partial
from os.path import basename
from threading import Thread
import zlib
import uuid
import pika
import zipfile
import io
import os
import time
import sys
import types
import json


app = Flask(__name__)
app.config.from_pyfile('settings.py')

ALLOWED_EXTENSIONS = {"txt", "pdf", "png", "jpg", "jpeg", "gif", "zip", "dmg"}
BASE_DIR = os.path.abspath(os.path.dirname(__file__))


def compress_file(file_name, unique_id, file_path, route_key):
    time.sleep(1)
    in_file = os.path.join(app.root_path, "uploads/" + file_name)
    out_file = os.path.join(app.root_path, "compressed/" + file_name + ".zip")

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=app.config['MQ_HOST'],
            port=app.config['MQ_PORT'],
            virtual_host="/"+app.config['MQ_VHOST'],
            credentials=pika.PlainCredentials(
                username=app.config['MQ_USERNAME'],
                password=app.config['MQ_PASSWORD']
            ),
        ))
    channel = connection.channel()
    channel.exchange_declare(
        exchange=app.config['MQ_EXCHANGE_KEY'], exchange_type='direct')

    def send_message(message, route_key, exchange_key=app.config['MQ_EXCHANGE_KEY']):

        channel.basic_publish(exchange=exchange_key,
                              routing_key=route_key,
                              body=message)

        print("\n [x] Sent %r\n" % message)

    def progress(total_size, original_write, self, buf):
        time.sleep(0.1)
        progress.bytes += len(buf)
        percent = int(100 * progress.bytes / total_size)
        message = '{"status": "on_progress", "percentage": ' + \
            str(percent) + '}'
        send_message(message, route_key)
        return original_write(buf)

    progress.bytes = 0
    zip_file_name = ""
    with zipfile.ZipFile(out_file, 'w', compression=zipfile.ZIP_DEFLATED) as _zip:
        _zip.fp.write = types.MethodType(partial(progress, os.path.getsize(in_file),
                                                 _zip.fp.write), _zip.fp)
        _zip.write(in_file, file_name)

    message = '{"status": "on_progress", "percentage": ' + \
        str(100) + '}'
    send_message(message, route_key)
    time.sleep(0.2)
    message = '{"status": "completed", "message" : "Your file has been compressed!" ,"url": ' + '"' + \
        f"{app.config['UC_URL']}/compressed/{file_name}.zip" + '"' + '}'
    send_message(message, route_key)
    connection.close()
    return


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route("/", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        if "file" not in request.files:
            return jsonify({"message": "put your file", "status_code": 403})

        file = request.files["file"]

        if file.filename == "":
            return jsonify({"message": "forbidden", "status_code": 403})

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            unique_id = uuid.uuid1()
            file_name = f"{unique_id}.{filename.split('.')[1]}"
            file.save(os.path.join(app.root_path, "uploads/" + file_name))
            res = {
                "unique_id": unique_id,
                "message": "Upload Success!",
                "img_url": f"{app.config['UC_URL']}/uploads/{file_name}",
                "status_code": 200,
            }
            thread = Thread(target=compress_file, args=(
                file_name,
                unique_id,
                os.path.join(app.root_path, "uploads/" + file_name),
                request.headers.get('X-ROUTING-KEY')
            ))
            thread.daemon = True
            thread.start()

            return jsonify(res)
        else:
            return jsonify({"message": "File uploaded is not correct format", "status_code": 403})

    else:
        return jsonify({"sanity": "checked"})


@app.route("/uploads/<filename>")
def uploaded_file(filename):
    return send_from_directory("uploads", filename)


@app.route("/compressed/<filename>")
def compressed_file(filename):
    return send_from_directory("compressed", filename)


# Or specify port manually:
if __name__ == "__main__":
    port = int(os.environ.get("COMPRESS_SERVICE_PORT", 8081))
    app.run(host="0.0.0.0", port=port)
