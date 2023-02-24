import argparse

from local_model_tasks import initialization, find_experts


def parse_arguments():
    parser = argparse.ArgumentParser(description='Expert finder service.')
    parser.add_argument('--sentence', dest='sentence', required=True,
                        help='Sentence to submit to the model')
    parser.add_argument('--precision', dest='precision', required=True,
                        help='Semantic expansion coefficient')
    parser.add_argument('--model', dest='model', required=True,
                        help='Model, ada or bert')
    arguments, unknown = parser.parse_known_args()
    return arguments


if __name__ == '__main__':
    args = parse_arguments()
    initialization()
    result = find_experts(sentence=args.sentence, precision=float(args.precision))
    print(result)
