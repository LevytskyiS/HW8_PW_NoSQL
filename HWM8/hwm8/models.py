import connect
from mongoengine import EmbeddedDocument, Document
from mongoengine.fields import (
    EmbeddedDocumentField,
    ListField,
    StringField,
    ReferenceField,
)


class Authors(Document):
    fullname = StringField()
    born_date = StringField()
    born_location = StringField()
    description = StringField()


class Tags(EmbeddedDocument):
    name = StringField()


class Quotes(Document):
    tags = ListField(EmbeddedDocumentField(Tags))
    author = ReferenceField(Authors)
    quote = StringField()
    meta = {"allow_inheritance": True}
