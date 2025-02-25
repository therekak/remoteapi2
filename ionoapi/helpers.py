import time
from functools import wraps
from datetime import datetime, timedelta, timezone
import ciso8601
from furl import furl
from typing import TYPE_CHECKING, Dict, Optional, Union, Callable
import requests
from requests.auth import HTTPBasicAuth
import tenacity
from apiclient.utils.typing import BasicAuthType, OptionalStr, OptionalDict
from apiclient.request_strategies import Response, RequestsResponse, BaseRequestStrategy, \
    RequestStrategy as RequestStrategy_
from apiclient.authentication_methods import BaseAuthenticationMethod, NoAuthentication
from apiclient.exceptions import UnexpectedError
from apiclient.retrying import retry_if_api_request_error



msc_retry = tenacity.retry(
    retry=retry_if_api_request_error(status_codes=[401, 429, 500, 501, 503]),
    wait=tenacity.wait_fixed(5),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
)


def urljoin(*args, base):
    url = furl(base)
    if len(args):
        url.path.segments = [_ for _ in url.path.segments if _] + list(args)
    return url


def endpoint(*iargs, base):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            api = args[0]
            now = datetime.utcnow()
            if api._NEXT_REQUEST and now < api._NEXT_REQUEST:
                time.sleep((api._NEXT_REQUEST - now).total_seconds())
            res = f(*args, **kwargs, url = urljoin(*iargs, base=base))
            api._NEXT_REQUEST = datetime.utcnow() + timedelta(seconds=api._MIN_REQINTRVL)
            return res
        return wrapper
    return decorator


class HeaderAuthenticationJWT(BaseAuthenticationMethod):
    """Authentication provided within the header.

    Normally associated with Oauth authoriazation, in the format:
    "Authorization: Bearer <token>"
    """

    @property
    def expired(self):
        if self._expiration is None:
            return self._expiration
        now = datetime.now().astimezone(timezone.utc) - timedelta(seconds=5)
        return  bool(now>=self._expiration)

    def __init__(
        self,
        auth_url: str, username: str, password: str,
        #token: str,
        parameter: str = "Authorization",
        scheme: OptionalStr = "Bearer",
        extra: Optional[Dict[str, str]] = None,
    ):
        self._auth_url = auth_url
        self._username = username
        self._password = password

        self._token = None #token
        self._expiration = None
        self._parameter = parameter
        self._scheme = scheme
        self._extra = extra

    def get_headers(self) -> Dict[str, str]:
        if self._scheme:
            headers = {self._parameter: f"{self._scheme} {self._token}"}
        else:
            headers = {self._parameter: self._token}
        if self._extra:
            headers.update(self._extra)
        return headers

    @msc_retry
    def perform_initial_auth(self, client: "APIClient"):

        def _make_iauth_request(
                rs: RequestStrategyU,
                request_method: Callable,
                endpoint: str,
                params: OptionalDict = None,
                headers: OptionalDict = None,
                data: OptionalDict = None,
                **kwargs,
        ) -> Response:
            """Make the request with the given method.

            Delegates response parsing to the response handler.
            """
            try:
                response = RequestsResponse(
                    request_method(
                        endpoint,
                        params=rs._get_request_params(params),
                        headers=rs._get_request_headers(headers),
                        auth=HTTPBasicAuth(self._username, self._password),
                        data=rs._get_formatted_data(data),
                        timeout=rs._get_request_timeout(),
                        **kwargs,
                    )
                )
            except Exception as error:
                raise UnexpectedError(f"Error when contacting \'{endpoint}\'") from error
            else:
                rs._check_response(response)
            return rs._decode_response_data(response)

        resp = _make_iauth_request(client.get_request_strategy(), client.get_session().get, self._auth_url)
        self._token = resp['access_token']
        self._expiration = ciso8601.parse_datetime(resp['expires_at'])


class RequestStrategyU(RequestStrategy_):
    """Requests strategy that uses the `requests` lib with a `requests.session`."""

    def set_client(self, client: "APIClient"):
        super(RequestStrategy_, self).set_client(client)
        # Set a global `requests.session` on the parent client instance.
        if self.get_session() is None:
            _session = requests.session()
            _session.verify = False
            self.set_session(_session)

    def get_session(self):
        client = self.get_client()
        if client.token_expired is True:
            auth = client.get_authentication_method()
            auth.perform_initial_auth(client)
            client = self.get_client()
        return client.get_session()

    def _handle_bad_response(self, response: Response):
        """Convert the error into an understandable client exception."""
        client = self.get_client()
        exc = client.get_error_handler().get_exception(response)
        if exc.status_code==401:
            auth = client.get_authentication_method()
            auth.perform_initial_auth(client)
        elif exc.status_code==429:
            time.sleep(0.1)
        raise exc

    def __init__(self, *args, **kwargs):
        super(RequestStrategyU, self).__init__(*args, **kwargs)
