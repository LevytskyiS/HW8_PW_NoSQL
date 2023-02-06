import pika
import sys
import time
from model import Contact


credentials = pika.PlainCredentials("guest", "guest")
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost", port=5672, credentials=credentials)
)
channel = connection.channel()

channel.queue_declare(queue="task_queue", durable=True)
print(" Waiting for messages from 'producer.py'. To exit press CTRL+C")


def send_mail(contact_id):

    contacts = Contact.objects()

    for contact in contacts:
        if contact_id == str(contact.id):
            contact.update(send_notification=True)
            print(f"An e-mail was sent to {contact.fullname}")


def callback(ch, method, properties, body):

    message = body.decode()
    print(f" Received: {message}")
    send_mail(message)
    time.sleep(0.5)
    print(f" Done: {method.delivery_tag}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue="task_queue", on_message_callback=callback)

if __name__ == "__main__":
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(0)
