import os
from dotenv import load_dotenv
import hvac

load_dotenv()

VAULT_URL = os.getenv("VAULT_PATH")
USER = os.getenv("VAULT_USER")
PASSWORD = os.getenv("VAULT_PASSWORD")

def get_vault_token_userpass(username, password):
    client = hvac.Client(url=VAULT_URL)
    login_response = client.auth.userpass.login(
        username=username,
        password=password
    )
    if client.is_authenticated():
        print("Authentication successful")
        return login_response['auth']['client_token']
    else:
        print("Authentication failed")
        return None
    
def print_kbmckenzie_secret(token):
    client = hvac.Client(url=VAULT_URL, token=token)
    try:
        secret_response = client.secrets.kv.v2.read_secret_version(path="kbmckenzie", mount_point="kv")
        secret_data = secret_response['data']['data']
        username = secret_data.get('username')
        password = secret_data.get('password')
        print(f"kbmckenzie.username: {username}")
        print(f"kbmckenzie.password: {password}")
    except Exception as e:
        print(f"Failed to read secret from kv/kbmckenzie: {e}")

if __name__ == "__main__":
    if USER and PASSWORD:
        token = get_vault_token_userpass(USER, PASSWORD)
        if token:
            print(f"Token: {token}")
            print_kbmckenzie_secret(token)
        else:
            print("Failed to retrieve token.")
    else:
        print("Vault user or password not set in environment variables.")