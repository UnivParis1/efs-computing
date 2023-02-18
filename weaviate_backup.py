import argparse
from datetime import datetime

import weaviate
from dotenv import dotenv_values
from huggingface_hub.utils import logging

from log_handler import LogHandler
from mail_sender import MailSender

DEFAULT_PREFIX = 'efs'

weaviate_params = dict(dotenv_values(".env.weaviate"))


def get_client():
    return weaviate.Client(weaviate_params['host'], timeout_config=(1000, 1000))


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Loads HAL bibliographic references, authors and structures to vector database.')
    parser.add_argument('--prefix', dest='prefix',
                        help='Back filename prefix', required=False, default=DEFAULT_PREFIX)
    return parser.parse_args()


def main(args):
    global logger
    logger = LogHandler("weaviate_backup", 'log', 'weaviate_backup.log', logging.INFO).create_rotating_log()
    prefix = args.prefix
    folder_time = datetime.now().strftime("%Y%m%d-%H%M%S")
    try:
        # result = get_client().backup.create(
        #     backup_id=f"{prefix}-{folder_time}",
        #     backend="filesystem",
        #     include_classes=["AdaSentence", "SbertSentence", "Publication", "Author", "Organisation"],
        #     wait_for_completion=True,
        # )
        result = {'status': 'SUCCESS', 'path': 'Toto'}
        if result['status'] == 'SUCCESS':
            message = f"Successful backup to {result['path']}"
            logger.info(message)
            MailSender().send_email(text=message)
        else:
            logger.error(f"Backup failure with unknown status")
    except Exception as e:
        logger.exception(f"Backup failure {e}")


if __name__ == '__main__':
    main(parse_arguments())
