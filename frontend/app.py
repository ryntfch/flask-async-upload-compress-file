import os
import requests
import uuid
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)
app.config.from_pyfile('settings.py')


@app.route("/", methods=["GET", "POST"])
def main():
    if request.method == "POST":
        file_ = request.files["file"]
        files = {"file": (file_.filename, file_)}
        print(request.form.get('route_key'))
        headers = {"X-ROUTING-KEY": request.form.get('route_key')}
        r = requests.post(app.config['UC_URL'], files=files, headers=headers)
        return jsonify(res=r.json())
    else:
        return render_template("index.html",
                               FE_URL=app.config['FE_SERVICE_PORT'],
                               UC_URL=app.config['UC_SERVICE_PORT'],
                               HTTP_HOST_URL=app.config['HTTP_HOST_URL'],
                               WS_STOMP_URL=app.config['WS_STOMP_URL'],
                               WS_HOST_URL=app.config['WS_HOST_URL'],
                               MQ_EXCHANGE_KEY=app.config['MQ_EXCHANGE_KEY'],
                               MQ_HOST=app.config['MQ_HOST'],
                               MQ_PORT=app.config['MQ_PORT'],
                               MQ_WS_PORT=app.config['MQ_WS_PORT'],
                               MQ_USERNAME=app.config['MQ_USERNAME'],
                               MQ_PASSWORD=app.config['MQ_PASSWORD'],
                               MQ_VHOST=app.config['MQ_VHOST'])


if __name__ == "__main__":
    port = int(os.environ.get("UPLOAD_SERVICE_PORT", 8083))
    app.run(host="0.0.0.0", port=port)
