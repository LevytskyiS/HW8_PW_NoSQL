import json
import connect
from models import Tags, Authors, Quotes

with open("authors.json", "r", encoding="utf-8") as json_file:
    source = json.load(json_file)

for el in source:
    author = Authors(
        fullname=el.get("fullname"),
        born_date=el.get("born_date"),
        born_location=el.get("born_location"),
        description=el.get("description"),
    )
    author.save()

with open("quotes.json", "r", encoding="utf-8") as json_file:
    source_quotes = json.load(json_file)

for el in source_quotes:
    tags = []
    author_for_quotes = None
    quote = el.get("quote")

    for tag in el.get("tags"):
        tags.append(Tags(name=tag))

    for author in Authors.objects():
        if el.get("author") == author.fullname:
            author_for_quotes = author

    quote = Quotes(tags=tags, author=author_for_quotes, quote=quote)
    quote.save()
