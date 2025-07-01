import httpx


def _get_session(identifier: str, password: str) -> tuple[str, str] | None:
    baseurl = 'https://crucial-woodcock-33.clerk.accounts.dev/v1/client/'
    with httpx.Client() as c:
        url = baseurl + 'sign_ins?_is_native=true'
        print(url)
        headers = {'Content-Type': 'application/x-www-form-urlencoded', }

        data = {
            "identifier": identifier,
            "password": password,
            "strategy": "password",
        }
        response = c.post(url, headers=headers, data=data)
        if response.status_code == 200:
            response = response.json()
            return response['response']['created_session_id'], response['client']['sessions'][0]['last_active_token']['jwt']
        else:
            print("Login failed:", response)
            return None


class APIClient:
    def __init__(self, identifier, password):
        token = self._signin(identifier, password)
        print("Token:", token)
        self.token = token
        self.base_url = "https://rest.focused.uno"
        self.client = httpx.AsyncClient(base_url=self.base_url)
        self.identifier = identifier
        self.password = password

    async def request(self,  method: str = "GET", endpoint: str = "/", headers: dict | None = None, query: dict | None = None):
        if not self.token:
            print("Authentication failed. Cannot make request.")
            return None

        auth_header = {'Authorization': f"Bearer {self.token}"}
        headers = merge_dicts(auth_header, headers or {})

        url = f"{self.base_url}{endpoint}"

        try:
            response = await self.client.request(method, url, headers=headers, params=query)
            response.raise_for_status()
            return response.json()

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                self.onUnautheticated()
                await self.request(method, url, headers=headers, query=query)
            print(
                f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
            return None

    async def get_users(self, query_params: dict | None = None) -> dict | None:
        endpoint = "/user"
        return await self.request(method="GET", endpoint=endpoint, query=query_params)

    def _signin(self, identifier: str, password: str) -> str | None:
        session_info = _get_session(identifier, password)
        TEMPLATE_NAME = "jwt_template_v1"
        if session_info:
            session_id, last_token = session_info

            url = "https://api.focused.uno/tokenFromSession"

            headers = {"Authorization": f"Bearer {last_token}"}

            queryParams = {"sessionId": session_id, "template": TEMPLATE_NAME}

            response = httpx.get(url, params=queryParams, headers=headers)
            if response.status_code == 200:
                jwt = response.json().get('jwt')
                return jwt
            else:
                print("Login failed")
                return None
        else:
            print("Login failed")
            return None

    def onUnautheticated(self):
        print("Re-authenticating...")
        self.token = self._signin(self.identifier, self.password)
        if not self.token:
            print("Re-authentication failed.")
            return None
        return self.token


def merge_dicts(dict1, dict2):
    merged_dict = {}
    for key in dict1:
        if key in dict2:
            merged_dict[key] = [dict1[key], dict2[key]]
        else:
            merged_dict[key] = dict1[key]

    for key in dict2:
        if key not in merged_dict:
            merged_dict[key] = dict2[key]

    return merged_dict
