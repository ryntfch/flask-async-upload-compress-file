# Flask Upload and Compress File Asynchronously

## Author

[Fachry Nataprawira](https://github.com/icigo)

## Description

a flask app that let you to upload file and it will compress and send to your front end app the percentage progress using Rabbit MQ (Message Queueing) asyncrhonously

## Technology

1. [Flask](https://flask.palletsprojects.com/en/1.1.x/) (Back-end server and Front-end app)
2. [RabbitMQ](https://www.rabbitmq.com/documentation.html) (Message Queueing)
3. [StompJS](https://github.com/stomp-js/stompjs) (Stream Text Oriented Messaging Protocol)
4. [AJAX](https://api.jquery.com/category/ajax/) (Asynchronous Javascript)
5. [SockJs](https://github.com/sockjs) (WebSocket Emulation)


## How to run in

1. Prepare your environtment
```bash
virtualenv -p python3 venv
source venv/bin/activate
pip3 install -r requirements.txt
```

2. Start RabbitMQ
```bash
sudo rabbitmq-server
```

3. Create your local .env
```bash
cp .env.example frontend/.env
cp .env.example compress_service/.env
```

4. Start Front-end
```bash
cd compress_service; flask run
```

5. Start Compress Service
```bash
cd frontend; flaks run
```
