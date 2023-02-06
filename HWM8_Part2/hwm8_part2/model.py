import connect
from mongoengine import Document
from mongoengine.fields import StringField, BooleanField


class Contact(Document):
    fullname = StringField()
    email = StringField()
    send_notification = BooleanField(default=False)
