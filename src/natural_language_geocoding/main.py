import sys

from natural_language_geocoding.natural_earth import download_coastlines_file


def main():
    if len(sys.argv) > 1:
        action = sys.argv[1]
    else:
        print("Please provide an action. The only available action is 'init'.")
        exit(1)

    if action == "init":
        download_coastlines_file()
    else:
        print("Unknown action. Please use 'init'.")


if __name__ == "__main__":
    main()
