import dotenv
from hana_ml import dataframe
from datetime import datetime, timedelta


import os


def get_connection_context():
    dotenv.load_dotenv()
    hana_config = {
                "address": os.getenv("AIRMS_HOST"),
                "port": os.getenv("AIRMS_PORT"),
                "user": os.getenv("AIRMS_USER"),
                "databaseName": os.getenv("AIRMS_DATABASE"),
                "password":os.getenv('AIRMS_PASSWORD'),
                "encrypt": os.getenv("AIRMS_ENCRYPT"),
                "sslValidateCertificate": os.getenv("AIRMS_SSL_VALIDATE_CERTIFICATE"),
                "sslHostNameInCertificate": os.getenv("AIRMS_SSL_HOSTNAME_IN_CERT"),
                "sslTrustStore": os.getenv("AIRMS_SSL_TRUSTSTORE"),
                "connectTimeout": os.getenv("AIRMS_CONNECT_TIMEOUT"),
                "currentSchema": "CDMPHI"
            }
    conn = dataframe.ConnectionContext(**hana_config)
    return conn

def get_current_german_datetime_string():
    current_german_datetime = datetime.now()+ timedelta(hours=6)
    formatted_datetime = current_german_datetime.strftime("%Y-%m-%d-%H:%M") 
    return formatted_datetime


if __name__ == "__main__":
    print(get_current_german_datetime_string())