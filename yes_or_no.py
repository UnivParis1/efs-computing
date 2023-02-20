def yes_or_no(question):
    answer = input(question + "(y/n): ").lower().strip()
    print("")
    while not (answer == "y" or answer == "yes" or \
               answer == "n" or answer == "no"):
        print("Input yes or no")
        answer = input(question + "(y/n):").lower().strip()
        print("")
    if answer[0] == "y":
        return True
    else:
        return False
