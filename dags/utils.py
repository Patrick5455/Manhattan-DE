import os

import boto3
import yaml
from botocore.client import BaseClient
from dotenv import load_dotenv

load_dotenv()


class S3Config:
    def __init__(self, s3_config_dict: dict = None):
        if s3_config_dict is None or len(s3_config_dict) == 0:
            raise Exception("s3 configs map passed is none or empty")
        else:
            print("S3 configs map successfully loaded")
            self._access_key = s3_config_dict.get('ACCESS_KEY')
            self._secret_key = s3_config_dict.get('SECRET_KEY')
            self._s3_bucket_name = s3_config_dict.get('BUCKET_NAME')
            self._s3_region = s3_config_dict.get('REGION')

    @property
    def access_key(self):
        return self._access_key

    @access_key.setter
    def access_key(self, value):
        self._access_key = value

    @property
    def secret_key(self):
        return self._secret_key

    @secret_key.setter
    def secret_key(self, value):
        self._secret_key = value

    @property
    def s3_bucket_name(self):
        return self._s3_bucket_name

    @s3_bucket_name.setter
    def s3_bucket_name(self, value):
        self._s3_bucket_name = value

    @property
    def s3_region(self):
        return self._s3_region

    @s3_region.setter
    def s3_region(self, value):
        self._s3_region = value


class DBConfig:
    def __init__(self, db_config_dict):
        if db_config_dict is None or len(db_config_dict) == 0:
            raise Exception("DB configs map passed is none or empty")
        else:
            print("DB configs map successfully loaded")
            self._db_user = db_config_dict.get('DB_USER')
            self._db_password = db_config_dict.get('DB_PASSWORD')
            self._db_host = db_config_dict.get('DB_HOST')
            self._db_port = db_config_dict.get('DB_PORT')
            self._db_name = db_config_dict.get('DB_NAME')
            self._staging_db_schema = db_config_dict.get('STAGING_DB_SCHEMA')
            self._analytics_db_schema = db_config_dict.get('ANALYTICS_DB_SCHEMA')

    @property
    def db_host(self):
        return self._db_host

    @db_host.setter
    def db_host(self, value):
        self._db_host = value

    @property
    def db_port(self):
        return self._db_port

    @db_port.setter
    def db_port(self, value):
        self._db_port = value

    @property
    def db_name(self):
        return self._db_name

    @db_name.setter
    def db_name(self, value):
        self._db_name = value

    @property
    def staging_db_schema(self):
        return self._staging_db_schema

    @staging_db_schema.setter
    def staging_db_schema(self, value):
        self._staging_db_schema = value

    @property
    def analytics_db_schema(self):
        return self._analytics_db_schema

    @analytics_db_schema.setter
    def analytics_db_schema(self, value):
        self._analytics_db_schema = value

    @property
    def db_user(self):
        return self._db_user

    @db_user.setter
    def db_user(self, value):
        self._db_user = value

    @property
    def db_password(self):
        return self._db_password

    @db_password.setter
    def db_password(self, value):
        self._db_password = value


def yaml_configs_loader(config_file: str, parent_level: str = None) -> dict:
    print(f"config_file: {config_file}")
    with open(config_file, "r") as file:
        config_dict = yaml.safe_load(file)
    if parent_level is None:
        print("successfully loaded yaml configs file into a map")
        return config_dict
    else:
        print(f"successfully loaded yaml configs file at parent level {parent_level} into a map")
        return config_dict[parent_level]


def load_db_configs_in_dict() -> dict:
    config_dict = {
        'DB_HOST': os.getenv('DB_HOST'),
        'DB_PORT': os.getenv('DB_PORT'),
        'DB_NAME': os.getenv('DB_NAME'),
        'STAGING_DB_SCHEMA': os.getenv('STAGING_DB_SCHEMA'),
        'ANALYTICS_DB_SCHEMA': os.getenv('ANALYTICS_DB_SCHEMA'),
        'DB_USER': os.getenv('DB_USER'),
        'DB_PASSWORD': os.getenv('DB_PASSWORD')
    }
    print("successfully loaded db configs into a map")
    return config_dict


def load_s3_configs_in_dict() -> dict:
    config_dict = {
        'ACCESS_KEY': os.getenv('AWS_ACCESS_KEY'),
        'SECRET_KEY': os.getenv('AWS_SECRET_KEY'),
        'BUCKET_NAME': os.getenv('S3_BUCKET_NAME'),
        'REGION': os.getenv('S3_REGION')
    }
    print("successfully loaded s3 configs into a map")
    return config_dict


def get_s3_instance() -> BaseClient:

    s3_config_obj = S3Config(load_s3_configs_in_dict())

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=s3_config_obj.access_key,
        aws_secret_access_key=s3_config_obj.secret_key,
        region_name=s3_config_obj.s3_region,
        use_ssl=True
    )
    print("connected to AWS successfully")
    try:
        s3_client.create_bucket(Bucket=s3_config_obj.s3_bucket_name)
        pass
    except Exception as e:
        print("Error creating S3 bucket:", e)
        raise Exception("Error creating S3 bucket:", e)

    return s3_client


