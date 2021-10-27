import pandas as pd  # type: ignore
import mailparser  # type: ignore
import boto3  # type: ignore
import io  # type: ignore
from slugify import slugify  # type: ignore
import time
import sys
import traceback
import base64
import logging
import os
import awswrangler as wr  # type: ignore
import json

TEMPORARY_LAMBDA_FOLDER = "tmp"

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def notify_team(error='error'):
    logger.critical(f"notify team with error {error}")


class EmailParserInstance:
    def __init__(self, paths):

        self.bucket_name = paths['s3']['bucket']['name']
        self.email_key = paths['s3']['object']['key']
        self.email_name = slugify(paths['s3']['object']['key'].split('/')[-1])
        self.email_parsed = self.load_email_parser()
        self.lambda_download_path = f"/{TEMPORARY_LAMBDA_FOLDER}/{self.email_name}"
        self.STATUS_PUSHED = 0
        self.CONFIG_PARSER_KEY = "config/email.json"
        self.POSSIBLE_EXTENSION_FILE = self.S3_PREFIX_QUARANTINE = self.S3_PREFIX_CURATED = None
        self.load_config_parser()

    def load_config_parser(self):
        obj = s3_resource.Object(self.bucket_name, self.CONFIG_PARSER_KEY)
        email_configuration = json.loads(obj.get()['Body'].read())
        self.POSSIBLE_EXTENSION_FILE = email_configuration.get('POSSIBLE_EXTENSION_FILE',
                                                               os.environ.get('POSSIBLE_EXTENSION_FILE', "None").split(
                                                                   ','))
        self.S3_PREFIX_QUARANTINE = email_configuration.get('S3_PREFIX_QUARANTINE',
                                                            os.environ.get('S3_PREFIX_QUARANTINE'))
        self.S3_PREFIX_CURATED = email_configuration.get('S3_PREFIX_CURATED', os.environ.get('S3_PREFIX_CURATED'))

    def load_email_parser(self):
        obj = s3_resource.Object(self.bucket_name, self.email_key)
        email_bytes = obj.get()['Body'].read()
        mail = mailparser.parse_from_bytes(email_bytes)
        return mail

    def get_attachments_email(self):
        mail_attachment = self.email_parsed.attachments
        for attachment in mail_attachment:
            attachment['metadata'] = {}
            attachment['metadata']['received_from'] = slugify(self.email_parsed.from_[0][1])  # get only the email
            attachment['metadata']['sent_to'] = slugify(self.email_parsed.to[0][1])  # get only the email
            attachment['metadata']['received_date'] = str(self.email_parsed.date)
            yield attachment

    def push_email_in_quarantine(self):
        if self.STATUS_PUSHED == 0:
            try:
                s3_resource.Object(self.bucket_name, f'{self.S3_PREFIX_QUARANTINE}/emails/{self.email_name}').copy_from(
                    CopySource=self.email_key)

                time.sleep(1)  # wait for copy before tagging

                s3_client.put_object_tagging(
                    Bucket=self.bucket_name,
                    Key=f"{self.S3_PREFIX_QUARANTINE}/emails/{self.email_name}",
                    Tagging={
                        'TagSet': [{
                            'Key': "Project",
                            'Value': "EmailIntegration",
                        }]
                    },
                )
                error = 'to_be_defined'
                notify_team(error)
                self.STATUS_PUSHED = 1
            except s3_client.exceptions.ClientError as e:
                logging.error(f'put_object_in_quarantine {e}')
                return False
        return True


class AttachmentParserInstance:
    def __init__(self, parent_email: EmailParserInstance, attachment):
        self.parent_email = parent_email
        self.attachment = attachment
        self.attachment_filename = slugify(attachment['filename'])
        self.S3_PREFIX_CURATED = self.parent_email.S3_PREFIX_CURATED
        self.S3_PREFIX_QUARANTINE = self.parent_email.S3_PREFIX_QUARANTINE

    def read_dataframe(self):
        file_extension = self.attachment['filename'].split('.')[-1].lower()
        payload = base64.b64decode(self.attachment['payload'])
        attachment_content = io.BytesIO(payload)
        if file_extension == 'csv':
            df = pd.read_csv(attachment_content, low_memory=False)
            return df
        elif file_extension in ['xls', 'xlsx']:
            df = pd.read_excel(io=attachment_content)
            return df
        else:
            logger.info('can not read the dataframe - UnknownExtension ')
            self.push_attachment_in_quarantine()
            logger.critical(f"UnknownExtension Skip the attachment - {self.attachment['filename']}")
            raise Exception('UnknownExtension')

    def push_attachment_in_curated(self, pandas_data_frame):
        # 2021-01-11 07:29:38 =>  20210111

        partition_key_date = self.attachment['metadata']['received_date'].split()[0].replace('-', '')

        # To be used in case we would like to store the attachment in their original extension and not in Parquet
        object_attachment_name = f"original_{self.S3_PREFIX_CURATED}/{partition_key_date}/" \
                                 f"{self.parent_email.email_parsed.from_[0][1]}/" \
                                 f"{self.attachment_filename}"

        s3_client.upload_file(f"/{TEMPORARY_LAMBDA_FOLDER}/{self.attachment_filename}",
                              self.parent_email.bucket_name,
                              object_attachment_name,
                              ExtraArgs={'Metadata': self.attachment['metadata']}
                              )

        param = {
            "source": "emailParserSystem",
            "sender": self.parent_email.email_parsed.from_[0][1]
        }
        res = wr.s3.to_parquet(
            df=pandas_data_frame,
            path=f"s3://{self.parent_email.bucket_name}/"
                 f"{self.S3_PREFIX_CURATED}/"
                 f"{self.attachment_filename}",
            dataset=True,
            database=os.environ.get('GLUE_DATABASE_NAME'),
            table=self.attachment_filename,
            mode="append",
            description="Table created automatically from the email parser system",
            parameters=param
            # columns_comments=comments Add meta data on the columns if needed
        )
        logger.info(f'Attachment pushed to S3 {res}')
        time.sleep(1)  # wait for put before tagging
        for _sub_paths in res['paths']:
            s3_client.put_object_tagging(
                Bucket=self.parent_email.bucket_name,
                Key=_sub_paths.split(f's3://{self.parent_email.bucket_name}/')[1],
                Tagging={
                    'TagSet': [{
                        'Key': "Project",
                        'Value': "EmailIntegration",
                    }]
                },
            )

    def push_attachment_in_quarantine(self):
        try:
            payload = base64.b64decode(self.attachment['payload'])

            with open(f"/{TEMPORARY_LAMBDA_FOLDER}/{self.attachment_filename}", 'wb') as w:
                w.write(payload)
            s3_client.upload_file(f"/{TEMPORARY_LAMBDA_FOLDER}/{self.attachment_filename}",
                                  self.parent_email.bucket_name,
                                  f"{self.S3_PREFIX_QUARANTINE}/attachment/{self.attachment_filename}",
                                  ExtraArgs={'Metadata': self.attachment['metadata']})
            time.sleep(1)  # wait for copy before tagging

            s3_client.put_object_tagging(
                Bucket=self.parent_email.bucket_name,
                Key=f"'{self.S3_PREFIX_QUARANTINE}/attachment/{self.attachment_filename}'",
                Tagging={
                    'TagSet': [{
                        'Key': "Project",
                        'Value': "EmailIntegration",
                    }]
                },
            )
            error = 'to_be_defined'
            notify_team(error)
        except s3_client.exceptions.ClientError as e:
            logging.error(f'put_object_in_quarantine {e}')
            return False
        return True

    def check_extension_attachment(self):
        file_extension = self.attachment['filename'].split('.')[-1].lower()
        if file_extension in self.POSSIBLE_EXTENSION_FILE:
            return True
        return False


def lambda_handler(event, context):
    print(f'event {event}')

    emails = [EmailParserInstance(paths=paths) for paths in event['Records']]
    quarantine_objects = []

    for _email in emails:
        try:
            for _attachment in _email.get_attachments_email():
                logger.info(f'Start process of {_attachment}')
                my_attachment_instance = AttachmentParserInstance(parent_email=_email, attachment=_attachment)
                try:
                    df = my_attachment_instance.read_dataframe()
                    logger.info(f'Data shape rows,cols :{df.shape}, column names {df.columns}')
                    df.to_csv(f"/{TEMPORARY_LAMBDA_FOLDER}/{my_attachment_instance.attachment_filename}", index=False)
                    # 2021-01-11 07:29:38 =>  20210111
                    my_attachment_instance.push_attachment_in_curated(df)
                except Exception as error:
                    if str(error) == "UnknownExtension":
                        quarantine_objects.append(_email)
                        _email.push_email_in_quarantine()
                        continue
                    else:
                        logger.critical(f"Unkown exception {error}")
                        notify_team(str(error))

        except Exception as error:
            exc_type, exc_value, exc_tb = sys.exc_info()
            logger.critical(traceback.format_exception(exc_type, exc_value, exc_tb))
            logger.critical(f'Error in {_email.email_name} reading the s3 object {error}')
            quarantine_objects.append(_email)
            _email.push_email_in_quarantine()

        logger.info(f'quarantine objects: {quarantine_objects}')
