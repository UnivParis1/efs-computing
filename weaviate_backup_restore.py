import argparse
import traceback
from datetime import datetime

import weaviate
from dotenv import dotenv_values
from huggingface_hub.utils import logging

from log_handler import LogHandler
from mail_sender import MailSender
from yes_or_no import yes_or_no

DEFAULT_PREFIX = 'efs'

weaviate_params = dict(dotenv_values(".env.weaviate"))


def get_client():
    return weaviate.Client(weaviate_params['host'], timeout_config=(1000, 1000))


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Loads HAL bibliographic references, authors and structures to vector database.')
    parser.add_argument('--backup', action='store_true')
    parser.add_argument('--restore', action='store_true')
    parser.add_argument('--prefix', dest='prefix',
                        help='Back filename prefix', required=False, default=DEFAULT_PREFIX)
    parser.add_argument('--backup_id', dest='backup_id',
                        help='Backup id to restore from', required=False, default=None)
    return parser.parse_args()


def main(args):
    global logger
    logger = LogHandler("weaviate_backup", 'log', 'weaviate_backup.log', logging.INFO).create_rotating_log()
    if args.restore:
        backup_id = args.backup_id
        if backup_id is None:
            print("Mandatory argument backup_id for backup restoration")
            return
        if not yes_or_no(f"Are you sure you want to restore from {backup_id} ? "):
            print("Aborting backup restoration")
            return
        try:
            restore(backup_id)
        except Exception as e:
            message = f"Restoration failure : {e}"
            print(message)
    elif args.backup:
        try:
            prefix = args.prefix
            backup(prefix)
        except Exception as e:
            logger.exception(f"Backup failure : {e}")
            MailSender().send_email(type=MailSender.ERROR, text=f"Backup failure : {e}\n{traceback.format_exc()}")
    else:
        print("No operation requested")


def backup(prefix):
    folder_time = datetime.now().strftime("%Y%m%d-%H%M%S")
    result = get_client().backup.create(
        backup_id=f"{prefix}-{folder_time}",
        backend="filesystem",
        include_classes=["AdaSentence", "SbertSentence", "Publication", "Author", "Organisation"],
        wait_for_completion=True,
    )
    if result['status'] == 'SUCCESS':
        message = f"Successful backup of EFS weaviate database to {result['path']}"
        logger.info(message)
        MailSender().send_email(type=MailSender.INFO, text=message)
    else:
        raise RuntimeError("Backup failure with unknown reason")


def restore(backup_id):
    client = get_client()
    client.schema.delete_all()
    result = client.backup.restore(
        backup_id=backup_id,
        backend="filesystem",
        include_classes=["AdaSentence", "SbertSentence", "Publication", "Author", "Organisation"],
        wait_for_completion=True,
    )
    if result['status'] == 'SUCCESS':
        message = f"Successful restoration of EFS weaviate database from id {backup_id}"
        logger.info(message)
    else:
        raise RuntimeError(f"Restoration from id {backup_id} failed with unknown reason")


if __name__ == '__main__':
    main(parse_arguments())
