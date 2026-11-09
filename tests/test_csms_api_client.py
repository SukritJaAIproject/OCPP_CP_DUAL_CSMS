"""Offline API scope/transport checks; no real credentials or remote requests."""
import io
import json
from email.message import Message
from urllib.error import HTTPError
from urllib.request import HTTPCookieProcessor, Request

import pytest

from tools import csms_api_client as api


class Response:
    def __init__(self, body=b'{}', content_type='application/json', status=200):
        self.body, self.status = body, status
        self.headers = Message()
        self.headers['Content-Type'] = content_type

    def read(self, bound):
        return self.body[:bound]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class Opener:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def client_with(*responses, server='A'):
    client = api.CSMSAPI(server)
    client.opener = Opener(*responses)
    return client


def session(server='A', **cookie_fields):
    target = api.SERVERS[server]
    cookie = {'name': 'csms_session', 'value': 'synthetic-session-for-offline-test',
              'domain': api.urlsplit(target['base_url']).hostname, 'path': '/',
              'secure': target['base_url'].startswith('https:'), 'expires': None,
              'domain_specified': False}
    cookie.update(cookie_fields)
    return {'base_url': target['base_url'], 'charger_id': target['charger_id'], 'cookies': [cookie]}


def sent_body(client):
    return json.loads(client.opener.requests[-1][0].data)


def test_unknown_server_and_cross_server_session_are_rejected_before_network():
    with pytest.raises(ValueError, match='dedicated'):
        api.CSMSAPI('production')
    with pytest.raises(ValueError, match='destination'):
        api.CSMSAPI('A', session('B'))
    bad = session()
    bad['charger_id'] = 'CP-A-0001'
    with pytest.raises(ValueError, match='destination'):
        api.CSMSAPI('A', bad)


@pytest.mark.parametrize('fields', [{'name': 'unrelated'}, {'domain': 'evil.invalid'},
                                   {'domain': 'example.com'}, {'domain': 'sub.csms-a.example.com'}])
def test_saved_cookie_must_match_exact_csms_host(fields):
    with pytest.raises(ValueError, match='cookie scope'):
        api.CSMSAPI('A', session(**fields))


def test_cookiejar_handles_real_set_cookie_header_without_browser_store():
    client = api.CSMSAPI('A')
    headers = Message()
    headers['Set-Cookie'] = 'csms_session=synthetic-login-token; Path=/; HttpOnly; Secure; SameSite=Lax'

    class CookieResponse:
        def info(self):
            return headers

    client.jar.extract_cookies(CookieResponse(), Request(client.base + '/login'))
    exported = client.export_session()
    assert exported['charger_id'] == api.SERVERS['A']['charger_id']
    assert [row['name'] for row in exported['cookies']] == ['csms_session']
    restored = api.CSMSAPI('A', exported)
    request = Request(restored.base + '/api/ops/me')
    restored.jar.add_cookie_header(request)
    assert request.get_header('Cookie') == 'csms_session=synthetic-login-token'
    other = Request(api.SERVERS['B']['base_url'] + '/api/ops/me')
    restored.jar.add_cookie_header(other)
    assert other.get_header('Cookie') is None


def test_client_installs_cookie_and_same_origin_redirect_processors():
    client = api.CSMSAPI('A')
    assert any(isinstance(handler, HTTPCookieProcessor) for handler in client.opener.handlers)
    assert any(isinstance(handler, api._SameOriginRedirect) for handler in client.opener.handlers)


@pytest.mark.parametrize('destination', ['https://evil.invalid/login',
    'http://csms-a.example.com/login', 'https://csms-a.example.com:8443/login',
    'https://csms-b.example.com:8080/login'])
def test_redirect_refuses_host_scheme_or_port_change(destination):
    handler = api._SameOriginRedirect(api.SERVERS['A']['base_url'])
    request = Request(api.SERVERS['A']['base_url'] + '/login', data=b'password=synthetic')
    with pytest.raises(api.APIError, match='cross-origin'):
        handler.redirect_request(request, None, 302, 'Found', Message(), destination)


def test_same_origin_login_redirect_becomes_get_without_password_body():
    handler = api._SameOriginRedirect(api.SERVERS['A']['base_url'])
    request = Request(api.SERVERS['A']['base_url'] + '/login', data=b'password=synthetic')
    redirected = handler.redirect_request(request, None, 302, 'Found', Message(),
        api.SERVERS['A']['base_url'] + '/')
    assert redirected.get_method() == 'GET'
    assert redirected.data is None


@pytest.mark.parametrize('path', ['https://evil.invalid/', '//evil.invalid/', 'api/ops/me'])
def test_request_refuses_absolute_or_unrooted_paths(path):
    client = client_with()
    with pytest.raises(ValueError, match='pinned'):
        client._request(path)
    assert client.opener.requests == []


def test_command_pins_id_and_preserves_ocpp_rejection_for_caller():
    expected = {'success': True, 'result': {'status': 'Rejected'}}
    client = client_with(Response(json.dumps(expected).encode()), server='B')
    assert client.command('remote_start', {'connector_id': 2, 'id_tag': 'OFFLINE-TEST'}) == expected
    assert sent_body(client) == {'cp_id': api.SERVERS['B']['charger_id'], 'command': 'remote_start',
                                 'params': {'connector_id': 2, 'id_tag': 'OFFLINE-TEST'}}
    request, timeout = client.opener.requests[0]
    assert request.full_url == api.SERVERS['B']['base_url'] + '/api/command'
    assert timeout == 35
    assert request.get_header('Content-type') == 'application/json'


@pytest.mark.parametrize('field', ['cp_id', 'cps', 'url', 'base_url', 'charger_id'])
def test_command_rejects_nested_destination_override(field):
    client = client_with()
    with pytest.raises(ValueError, match='override'):
        client.command('remote_start', {field: 'production'})
    assert client.opener.requests == []


@pytest.mark.parametrize('command', ['raw', 'update_firmware', 'set_config', 'switch_version'])
def test_commands_cannot_choose_unscoped_or_shared_default_mutation(command):
    client = client_with()
    with pytest.raises(ValueError, match='Unsupported'):
        client.command(command)
    assert client.opener.requests == []


def test_configuration_uses_direct_batch_route_with_pinned_id():
    expected = {'success': True, 'results': [{'key': 'HeartbeatInterval', 'status': 'Accepted'}]}
    client = client_with(Response(json.dumps(expected).encode()))
    changes = [{'key': 'HeartbeatInterval', 'value': '31'}]
    assert client.configuration(changes) == expected
    assert client.opener.requests[-1][0].full_url.endswith('/api/config/batch')
    assert sent_body(client) == {'cp_id': client.charger_id, 'configs': changes}


@pytest.mark.parametrize('updates', [None, {}, [], [{'key': 'x', 'value': 12}],
    [{'key': '', 'value': '1'}], [{'key': 'x', 'value': '1', 'cp_id': 'production'}],
    [{'key': 'x', 'value': '1'}] * 51])
def test_configuration_requires_explicit_string_key_values(updates):
    client = client_with()
    with pytest.raises(ValueError):
        client.configuration(updates)
    assert client.opener.requests == []


def test_firmware_and_diagnostics_pin_targets_and_reject_arbitrary_download():
    client = client_with(Response(), Response(), server='B')
    client.firmware('http://127.0.0.1:8766/firmware/synthetic.bin')
    assert sent_body(client)['cps'] == [client.charger_id]
    client.diagnostics()
    assert sent_body(client)['cp'] == client.charger_id
    with pytest.raises(ValueError, match='synthetic'):
        client.firmware('https://production.invalid/firmware.bin')
    assert len(client.opener.requests) == 2


def test_points_and_firmware_response_filter_out_other_charger_data():
    client = client_with(server='B')
    rows = [{'id': client.charger_id, 'transactions': {'42': {'connector_id': 2}}},
            {'id': 'CP-B-0001', 'transactions': {'999': {'connector_id': 1}}}]
    jobs = {'fw_jobs': [{'cp': client.charger_id}, {'cp': 'CP-B-0001'}],
            'diag_jobs': [{'cp_id': client.charger_id}], 'diag_files': [{'charger_id': client.charger_id}],
            'files': [{'url': 'private-unrelated-data'}]}
    client.opener.responses.extend([Response(json.dumps(rows).encode()), Response(json.dumps(jobs).encode())])
    assert client.points() == rows[:1]
    assert client.fwdiag() == {key: jobs[key][:1] for key in ('fw_jobs', 'diag_jobs', 'diag_files')}


@pytest.mark.parametrize('payload,method', [({}, 'points'), ([1], 'points'), ([], 'fwdiag'),
    ({'fw_jobs': {}}, 'fwdiag'), ({'diag_jobs': [1]}, 'fwdiag')])
def test_malformed_collection_shapes_are_rejected(payload, method):
    client = client_with(Response(json.dumps(payload).encode()))
    with pytest.raises(ValueError, match='Unexpected'):
        getattr(client, method)()


@pytest.mark.parametrize('code', [401, 403, 404, 500])
def test_http_errors_are_sanitized_and_preserve_status(code):
    client = client_with(HTTPError('https://irrelevant.invalid', code,
        'sensitive-server-error', Message(), io.BytesIO(b'sensitive-error-body')))
    with pytest.raises(api.APIError) as failure:
        client.me()
    assert failure.value.status == code
    assert 'sensitive' not in str(failure.value)


@pytest.mark.parametrize('response,message', [
    (Response(b'<html>private login body</html>', 'text/html'), 'expected JSON'),
    (Response(b'private invalid JSON'), 'invalid JSON'),
    (Response(b'\xff'), 'invalid JSON'),
    (Response(b'x' * (8 * 1024 * 1024 + 1)), 'exceeds test bound')])
def test_bad_responses_raise_sanitized_errors(response, message):
    client = client_with(response)
    with pytest.raises(api.APIError, match=message) as failure:
        client.me()
    assert 'private' not in str(failure.value)


def test_login_encodes_form_then_checks_authenticated_identity_and_cookie():
    client = client_with(Response(b'<html>landing</html>', 'text/html'),
                         Response(b'{"role":"admin"}'))
    cookie = api.CSMSAPI('A', session()).jar
    client.jar = cookie
    assert client.login('test user', 'synthetic&password') == {'role': 'admin'}
    request = client.opener.requests[0][0]
    assert request.data == b'username=test+user&password=synthetic%26password'
    assert request.get_header('Content-type') == 'application/x-www-form-urlencoded'
    assert client.opener.requests[1][0].full_url.endswith('/api/ops/me')


def test_login_requires_actual_session_cookie_even_when_me_returns_json():
    client = client_with(Response(b'', 'text/html'), Response(b'{"role":"admin"}'))
    with pytest.raises(api.APIError, match='did not return a session'):
        client.login('test', 'synthetic')


def test_old_server_me_falls_back_only_after_404_with_existing_session_cookie():
    client = api.CSMSAPI('B', session('B'))
    client.opener = Opener(HTTPError(client.base + '/api/ops/me', 404, 'missing', Message(), io.BytesIO()),
                           Response(b'[]'))
    assert client.me() == {'authenticated': True, 'role': None, 'verification_endpoint': '/api/charge_points'}
    assert [request.full_url for request, _ in client.opener.requests] == [
        client.base + '/api/ops/me', client.base + '/api/charge_points']


@pytest.mark.parametrize('code,with_cookie', [(401, True), (403, True), (500, True), (404, False)])
def test_me_fallback_does_not_hide_auth_or_other_failures(code, with_cookie):
    client = api.CSMSAPI('B', session('B') if with_cookie else None)
    client.opener = Opener(HTTPError(client.base + '/api/ops/me', code, 'failed', Message(), io.BytesIO()), Response(b'[]'))
    with pytest.raises(api.APIError) as failure:
        client.me()
    assert failure.value.status == code
    assert len(client.opener.requests) == 1


@pytest.mark.parametrize('fallback', [Response(b'{"error":"not a list"}'), Response(b'null'),
    Response(b'<html>login</html>', 'text/html')])
def test_old_server_fallback_requires_authenticated_json_charge_point_list(fallback):
    client = api.CSMSAPI('B', session('B'))
    client.opener = Opener(HTTPError(client.base + '/api/ops/me', 404, 'missing', Message(), io.BytesIO()), fallback)
    with pytest.raises((ValueError, api.APIError)):
        client.me()


@pytest.mark.parametrize('value,normalized', [('Accepted', 'accepted'),
    ('RemoteStartStopStatus.accepted', 'accepted'), ('Rejected', 'rejected'),
    ('ConfigurationStatus.reboot_required', 'reboot_required'), ('NotSupported', 'notsupported')])
def test_ocpp_status_enum_and_plain_string_normalization(value, normalized):
    assert api.status(value) == normalized


def test_session_persistence_contains_only_supplied_session_structure(tmp_path, monkeypatch):
    calls = []

    def fake_dpapi(data, *, decrypt=False):
        calls.append(decrypt)
        return data[7:] if decrypt else b'CRYPTED' + data

    monkeypatch.setattr(api, '_dpapi', fake_dpapi)
    destination = tmp_path / 'session.dpapi'
    expected = {'A': session()}
    api.save_sessions(expected, destination)
    assert api.load_sessions(destination) == expected
    assert calls == [False, True]
    assert not destination.with_suffix('.tmp').exists()
