import os
import logging


def build_logger():
    # set up logging to file - see previous section for more details
    logging.basicConfig(level=logging.DEBUG, filename='file_name_validate.log', filemode='w')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)


def menu():
    print("This tool will check a given filesystem location recursively for invalid paths and filenames")
    print()
    pp = _get_path()
    try:
        os.stat(pp)
        return pp
    except OSError as e:
        print(e)
        menu()


def _get_path():
    path = input("Enter the path you would like to validate: ")
    return path


def check_path(pp, count):
    logger = logging.getLogger('check_path')
    if len(pp) > 260:
        logger.error("{} characters: {}".format(len(pp), pp.encode('utf-8')))
        count += 1
    else:
        print("Valid: {}".format(pp.encode('utf-8')))
    return count

if __name__ == "__main__":
    build_logger()
    logger = logging.getLogger('main')
    path = menu()
    count = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            count = check_path(os.path.join(root, f), count)

    logger.info("{} invalid file paths.".format(count))