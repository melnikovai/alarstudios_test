import requests


class AuthClass(object):
    def __init__(self, logger, user=None, password=None):
        self.logger = logger
        self._user = user
        self._password = password
        self._status = None
        self._code = None
        self._response = None

        if not any([self._user, self._password]):
            raise "Credentials has not been specified!"

    @property
    def user(self) -> str: return self._user

    @property
    def password(self) -> str: return "****"

    @property
    def status(self) -> str: return self._status

    @property
    def code(self) -> str: return self._code

    @property
    def response(self) -> str: return self._response

    def __str__(self) -> str:
        return f"\nAuthorization credentials:\n{self.user}\n{self.password}\n{self.status}\n{self.code}\n{self.response}"

    def get_response(self) -> str:
        url = "https://www.alarstudios.com/test/auth.cgi"
        headers = {"Accept": "application/json"}
        payload = {"username": self._user, "password": self._password}

        try:
            response = requests.get(
                url=url,
                headers=headers,
                params=payload,
                timeout=100
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)

        self._response = response.json()

        self.logger.debug(f"type is: {type(self._response)}")
        self.logger.debug(f"response is: {self._response}")
        try:
            self._status = self._response["status"]
            self._code = self._response["code"]
        except KeyError as ke:
            self.logger.warning(str(ke))

        return self.code
