import connect
from abstract_classes import COMMANDS


def handler(user_input: str) -> str:

    user_input_list = user_input.split(":")
    command = user_input_list[0]
    values = user_input_list[1:]

    if command in COMMANDS.keys():
        return COMMANDS[command]().command_to_execute(values)
    else:
        return f"Such command ({command}) doesn't exist."


def main():

    while True:

        user_input = input("Enter 'command:value' for search: ")
        result = handler(user_input)
        print(result)


if __name__ == "__main__":
    main()
