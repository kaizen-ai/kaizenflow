from flask import Flask
import pika

app = Flask(__name__)

@app.route('/')
def index():
  return 'OK'


@app.route('/create-job/<msg>')
def add(cmd):
  try:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
  except pika.exceptions.AMQPConnectionError as exc:
    print('Failed to connect to RabbitMQ service. Message wont be sent.')
    return


  channel = connection.channel()
  channel.queue_declare(queue='task_queue', durable=True)
  channel.basic_publish(
    exchange='',
    routing_key='task_queue',
    body=cmd,
    properties=pika.BasicProperties(delivery_mode=2,))

  connection.close()
  return "___ Sent: %s" % cmd


if __name__ == '__main__':
  app.run(debug=True, host='0.0.0.0')
