import secrets
from twisted_client import client


if __name__ == '__main__':
    client.get_vbucket_data(secrets.DCP_INSTANCE[0], 
                            secrets.DCP_INSTANCE[1], 0, secrets.DCP_CREDENTIALS)
