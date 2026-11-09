"""Task-scoped CSMS HTTP client. Never reads or controls a browser.

Only actual /login Set-Cookie sessions are accepted. Cookie persistence uses
Windows DPAPI for the current user; passwords are never persisted. The command
API and firmware/diagnostics wrappers pin the dedicated simulator identities.
"""
from __future__ import annotations

import ctypes
from ctypes import wintypes
from datetime import datetime, timezone
import http.cookiejar
import json
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, build_opener, HTTPCookieProcessor, HTTPRedirectHandler, ProxyHandler

ROOT = Path(__file__).resolve().parents[1]
AUTH_FILE = ROOT / '.api_auth/csms_sessions.dpapi'
SERVERS = {
    'A': {'base_url': 'https://csms-a.example.com', 'charger_id': 'CP-A-0001-TEST-PC-9F3A'},
    'B': {'base_url': 'http://csms-b.example.com:8080', 'charger_id': 'CP-B-0001-TEST-PC-9F3A'},
}
COMMANDS = {'remote_start', 'remote_stop', 'reset', 'unlock', 'availability', 'get_config',
            'clear_cache', 'data_transfer', 'trigger', 'reserve', 'cancel_reserve',
            'set_profile', 'clear_profile', 'get_composite', 'get_local_list', 'send_local_list', 'get_diagnostics'}


class APIError(RuntimeError):
    def __init__(self, status, kind):
        self.status = status
        super().__init__(f'CSMS API HTTP {status}: {kind}')


class _Blob(ctypes.Structure):
    _fields_ = [('cbData', wintypes.DWORD), ('pbData', ctypes.POINTER(ctypes.c_ubyte))]


def _dpapi(data, *, decrypt=False):
    if os.name != 'nt':
        raise RuntimeError('This session store uses Windows current-user DPAPI')
    if not isinstance(data, bytes) or len(data) > 65536:
        raise ValueError('Unexpected session-store data')
    buffer = (ctypes.c_ubyte * len(data)).from_buffer_copy(data)
    source, target = _Blob(len(data), buffer), _Blob()
    crypt = ctypes.WinDLL('crypt32', use_last_error=True)
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.LocalFree.argtypes = [ctypes.c_void_p]
    kernel.LocalFree.restype = ctypes.c_void_p
    if decrypt:
        ok = crypt.CryptUnprotectData(ctypes.byref(source), None, None, None, None, 1, ctypes.byref(target))
    else:
        ok = crypt.CryptProtectData(ctypes.byref(source), 'OCPP dedicated test API sessions', None, None, None, 1, ctypes.byref(target))
    if not ok:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        if target.cbData > 65536:
            raise ValueError('Unexpected decrypted session-store size')
        return ctypes.string_at(target.pbData, target.cbData)
    finally:
        kernel.LocalFree(target.pbData)


def save_sessions(sessions, destination=AUTH_FILE):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    encrypted = _dpapi(json.dumps({'version': 1, 'sessions': sessions}).encode('utf-8'))
    temporary = destination.with_suffix('.tmp')
    with temporary.open('wb') as stream:
        stream.write(encrypted)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, destination)


def load_sessions(source=AUTH_FILE):
    data = json.loads(_dpapi(Path(source).read_bytes(), decrypt=True))
    if data.get('version') != 1 or not isinstance(data.get('sessions'), dict):
        raise ValueError('Unexpected API session store')
    return data['sessions']


class _SameOriginRedirect(HTTPRedirectHandler):
    def __init__(self, base):
        self.origin = urlsplit(base)

    def redirect_request(self, request, fp, code, msg, headers, newurl):
        dest = urlsplit(newurl)
        if (dest.scheme, dest.hostname, dest.port) != (self.origin.scheme, self.origin.hostname, self.origin.port):
            raise APIError(code, 'cross-origin redirect refused')
        return super().redirect_request(request, fp, code, msg, headers, newurl)


class CSMSAPI:
    def __init__(self, server, session=None):
        if server not in SERVERS:
            raise ValueError('Use dedicated test CSMS A or B')
        self.server = server
        self.base = SERVERS[server]['base_url']
        self.charger_id = SERVERS[server]['charger_id']
        self.jar = http.cookiejar.CookieJar()
        if session is not None:
            if session.get('base_url') != self.base or session.get('charger_id') != self.charger_id:
                raise ValueError('Session destination does not match the pinned test CSMS')
            for row in session.get('cookies', []):
                if row.get('name') != 'csms_session' or row.get('domain', '').lstrip('.') != urlsplit(self.base).hostname:
                    raise ValueError('Unexpected session cookie scope')
                self.jar.set_cookie(http.cookiejar.Cookie(
                    version=0, name=row['name'], value=row['value'], port=None, port_specified=False,
                    domain=row['domain'], domain_specified=row.get('domain_specified', False), domain_initial_dot=row['domain'].startswith('.'),
                    path=row['path'], path_specified=True, secure=row['secure'], expires=row.get('expires'),
                    discard=row.get('expires') is None, comment=None, comment_url=None, rest={'HttpOnly': None}))
        self.opener = build_opener(ProxyHandler({}), HTTPCookieProcessor(self.jar), _SameOriginRedirect(self.base))

    @classmethod
    def authenticated(cls, server, source=AUTH_FILE):
        sessions = load_sessions(source)
        if server not in sessions:
            raise APIError(401, 'session missing; run AUTHENTICATE_CSMS_API.cmd locally')
        return cls(server, sessions[server])

    def _request(self, path, body=None, *, form=None):
        if not path.startswith('/') or path.startswith('//') or '://' in path:
            raise ValueError('Use an API path on the pinned CSMS')
        if form is not None and body is not None:
            raise ValueError('Choose form or JSON request data')
        data, headers = None, {'Accept': 'application/json', 'User-Agent': 'OCPP-Dedicated-Test-Runner/1'}
        if form is not None:
            data = urlencode(form).encode('utf-8')
            headers['Content-Type'] = 'application/x-www-form-urlencoded'
        elif body is not None:
            data = json.dumps(body).encode('utf-8')
            headers['Content-Type'] = 'application/json'
        try:
            with self.opener.open(Request(self.base + path, data=data, headers=headers), timeout=35) as response:
                content = response.read(8 * 1024 * 1024 + 1)
                if len(content) > 8 * 1024 * 1024:
                    raise APIError(response.status, 'response exceeds test bound')
                if path == '/login':
                    return None
                if 'json' not in response.headers.get('Content-Type', '').lower():
                    raise APIError(response.status, 'expected JSON; session may have expired')
                try:
                    return json.loads(content)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    raise APIError(response.status, 'invalid JSON response') from None
        except HTTPError as error:
            # Error body, headers and cookies are not copied into logs/exceptions.
            raise APIError(error.code, 'authentication/permission/request rejected') from None

    def login(self, username, password):
        self._request('/login', form={'username': username, 'password': password})
        if not any(c.name == 'csms_session' for c in self.jar):
            raise APIError(401, 'login did not return a session')
        return self.me()

    def export_session(self):
        host = urlsplit(self.base).hostname
        cookies = [{'name': c.name, 'value': c.value, 'domain': c.domain, 'path': c.path,
                    'secure': c.secure, 'expires': c.expires, 'domain_specified': c.domain_specified}
                   for c in self.jar if c.name == 'csms_session' and c.domain.lstrip('.') == host]
        if not cookies:
            raise APIError(401, 'no scoped session cookie')
        return {'base_url': self.base, 'charger_id': self.charger_id,
                'created_at_utc': datetime.now(timezone.utc).isoformat(), 'cookies': cookies}

    def me(self):
        try:
            result = self._request('/api/ops/me')
        except APIError as error:
            if error.status != 404 or not any(c.name == 'csms_session' and not c.is_expired() for c in self.jar):
                raise
            # Older CSMS versions have the authenticated base API but no OPS
            # feature. A missing feature is not proof of a failed login. Only
            # this 404 may fall back; 401/403 remain authentication failures.
            self.points()
            return {'authenticated': True, 'role': None, 'verification_endpoint': '/api/charge_points'}
        if not isinstance(result, dict):
            raise ValueError('Unexpected authenticated-user response')
        return result

    def points(self):
        rows = self._request('/api/charge_points')
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ValueError('Unexpected charge-points response')
        return [row for row in rows if row.get('id') == self.charger_id]

    def command(self, command, params=None):
        if command not in COMMANDS:
            raise ValueError('Unsupported test command')
        params = dict(params or {})
        if any(k in params for k in ('cp_id', 'cps', 'url', 'base_url', 'charger_id')):
            raise ValueError('Command cannot override the pinned charger destination')
        return self._request('/api/command', {'cp_id': self.charger_id, 'command': command, 'params': params})

    def configuration(self, updates):
        """Send CP changes without persisting the CSMS's shared defaults.

        The server's /api/command set_config uses the with_save method. Its
        /api/config/batch route instead calls cmd_change_configuration directly.
        Keep set_config out of COMMANDS so test calls cannot select that path.
        """
        if not isinstance(updates, list) or not updates or len(updates) > 50:
            raise ValueError('Use one to fifty explicit configuration changes')
        for row in updates:
            if (not isinstance(row, dict) or set(row) != {'key', 'value'}
                    or not isinstance(row['key'], str) or not row['key'].strip()
                    or not isinstance(row['value'], str)):
                raise ValueError('Configuration changes require only string key/value fields')
        return self._request('/api/config/batch', {'cp_id': self.charger_id, 'configs': updates})

    def firmware(self, url):
        if url not in {f'http://127.0.0.1:8766/firmware/{name}' for name in ('synthetic.bin', 'wrong-sha.bin', 'truncated.bin')}:
            raise ValueError('Firmware API tests accept only the local synthetic fixtures')
        return self._request('/api/ops/fw/update', {'cps': [self.charger_id], 'url': url,
                             'retrieve': None, 'retries': 0, 'interval': 0})

    def diagnostics(self):
        return self._request('/api/ops/diag/request', {'cp': self.charger_id, 'from': None, 'to': None, 'retries': 0, 'interval': 0})

    def fwdiag(self):
        data = self._request('/api/ops/fwdiag')
        if not isinstance(data, dict) or any(not isinstance(data.get(key, []), list)
                or any(not isinstance(row, dict) for row in data.get(key, []))
                for key in ('fw_jobs', 'diag_jobs', 'diag_files')):
            raise ValueError('Unexpected firmware/diagnostics response')
        return {key: [row for row in data.get(key, []) if row.get('cp_id', row.get('cp', row.get('charger_id'))) == self.charger_id]
                for key in ('fw_jobs', 'diag_jobs', 'diag_files')}


def status(value):
    return str(value).rsplit('.', 1)[-1].lower()
