from abc import ABC, abstractmethod
from models import Quotes


class UserCommand(ABC):
    @abstractmethod
    def command_to_execute(self):
        pass


class FindQuotesByName(UserCommand):
    def command_to_execute(self, args: list):

        name = args[0]
        author_quotes = []
        quotes = Quotes.objects()

        for data in quotes:
            if name == str(data.author.fullname):
                author_quotes.append(data.quote)

        if not author_quotes:
            return f"{name} is not in DB or he doesn't have any quotes"
        return author_quotes


class FindTag(UserCommand):
    def command_to_execute(self, args: list):

        tag = args[0]
        tag_quotes = []
        quotes = Quotes.objects()

        for data in quotes:
            for data_tag in data.tags:
                if tag == data_tag.name:
                    tag_quotes.append(data.quote)

        if not tag_quotes:
            return f"{tag} is not in DB or it doesn't have any quotes"
        return tag_quotes


class FindTags(UserCommand):
    def command_to_execute(self, args):
        result = []
        tags = args[0].split(",")
        quotes_by_list_of_tags = Quotes.objects(tags__name__in=tags)

        for quote_object in quotes_by_list_of_tags:
            result.append(quote_object.quote)

        if not result:
            return f"No such tags ({tags}) in the DB"
        return result


class Exit(UserCommand):
    def command_to_execute(self, *args):
        exit("Bye!")


COMMANDS = {"name": FindQuotesByName, "tag": FindTag, "tags": FindTags, "exit": Exit}
