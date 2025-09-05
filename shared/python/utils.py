# package imports
import sys

def get_week() -> int:
    """
    Gets the fantasy football week from user input.

    Returns:
        int: The fantasy football week.
    """
    while True:
        try:
            user_input = input("Enter the week number for which to run the script (or 'quit' to exit): ").strip()
            if user_input.lower() == "quit":
                print(f"Exiting the program.")
                sys.exit()

            week = int(user_input)
            if week < 1 or week > 14:
                print(f"Please enter a valid week of the fantasy season between 1 and 14.")
                continue

            return week
        
        except Exception as ex:
            print(f"Invalid input. Please enter a positive integer between one (1) and fourteen (14) representing a week of the fantasy season.")