import argparse

from tasks import find_experts


def parse_arguments():
    parser = argparse.ArgumentParser(description='Expert finder service.')
    parser.add_argument('--sentence', dest='sentence', required=True,
                        help='Sentence to submit to the model')
    arguments, unknown = parser.parse_known_args()
    return arguments


if __name__ == '__main__':
    args = parse_arguments()
    result = find_experts.delay(sentence=args.sentence)
    print(result.get(timeout=3))
