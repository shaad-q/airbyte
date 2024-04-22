import requests

class SyndigoAPIClient:
    def __init__(self, username, secret):
        self.url_base = "https://api.syndigo.com"
        self.username = username
        self.secret = secret
        self.auth = self.authenticate()

    def authenticate(self):
        response = requests.get(f"{self.url_base}/api/auth", params={"username": self.username, "secret": self.secret})
        response.raise_for_status()
        return response.json()
