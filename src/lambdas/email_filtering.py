import logging
import os

import boto3
import json

s3_resource = boto3.resource('s3')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def notify_team(error='error'):
    logger.critical(f"notify team with error {error}")

def lambda_handler(event, context):
    logger.info(f'event {event}')
    bucket_name = os.environ.get('BUCKET_NAME')
    config_parser_key = os.environ.get('CONFIG_PARSER_KEY')
    obj = s3_resource.Object(bucket_name, config_parser_key)
    email_configuration = json.loads(obj.get()['Body'].read())
    logger.info(f'email configuration : {email_configuration}')
    accepted_senders = email_configuration.get("ACCEPTED_SENDERS", "None").split(',')
    try:
        for ses_records in event['Records']:
            ses_event = ses_records['ses']
            email_source = ses_event['mail']['source']
            logger.info(f'Email received from {email_source}')
            if any(_accepted_source.lower() in email_source.lower() for _accepted_source in accepted_senders):
                logger.info(f'{email_source} accepted into the system')
            else:
                logger.info(f'{email_source} rejected')
                notify_team("email_source_rejected")
                return {'disposition': 'STOP_RULE_SET'}
    except Exception as error:
        logger.critical(f"Failed Lambda run {error}")
        notify_team("fail_lambda_run")
        return {'disposition': 'STOP_RULE_SET'}
