import faker
import pika
import connect
from model import Contact


NUMBER_OF_CONTACTS = 3


def generate_fake_data():

    fullname = []
    email = []
    fake_data = faker.Faker()

    for _ in range(NUMBER_OF_CONTACTS):
        fullname.append(fake_data.name())

    for _ in range(NUMBER_OF_CONTACTS):
        email.append(fake_data.email())

    return fullname, email


def fill_db(fullname, email):

    if len(fullname) == len(email):
        for name, mail in zip(fullname, email):
            contact = Contact(fullname=name, email=mail)
            contact.save()


def main():
    credentials = pika.PlainCredentials("guest", "guest")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672, credentials=credentials)
    )
    channel = connection.channel()

    channel.exchange_declare(exchange="task_mock", exchange_type="direct")
    channel.queue_declare(queue="task_queue", durable=True)
    channel.queue_bind(exchange="task_mock", queue="task_queue")

    contacts = Contact.objects()

    for contact in contacts:
        channel.basic_publish(
            exchange="task_mock",
            routing_key="task_queue",
            body=str(contact.id),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.TRANSIENT_DELIVERY_MODE
            ),
        )
        print(f"Sent message to 'consumer.py'")

    connection.close()


if __name__ == "__main__":
    fill_db(*generate_fake_data())
    main()
