# Pure protocol AST export for the SIMULATOR, not the charger application.
# Original CP.py SHA256: bea85fa1f0e017881c1f05db248ccf0af847257062d7c2e621738e48ea66d39a
def _normalize_autocharge_prefix(value):
    """Return a valid AutoCharge idTag prefix, or None if the value is not allowed."""
    text = str(value if value is not None else '').strip()
    if not text or len(text) > AUTOCHARGE_PREFIX_MAX_LEN:
        return None
    if any((ch not in AUTOCHARGE_PREFIX_ALLOWED for ch in text)):
        return None
    return text

def convert_timeformat(current_time):
    return current_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

def _write_configuration_file(path, value):
    """Commit a setting before publishing its new in-memory value.

    The PLC helper intentionally logs and swallows I/O errors for its older
    callers. OCPP configuration needs a failure signal instead of Accepted for
    a value that could not be persisted.
    """
    import tempfile
    target = os.path.abspath(os.fspath(path))
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, dir=os.path.dirname(target), prefix='.' + os.path.basename(target) + '.') as stream:
            temporary = stream.name
            stream.write(str(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        temporary = None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

def _parse_csv_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [x for x in (str(x).strip() for x in value) if x]
    s = str(value).strip()
    if s == '' or s.lower() in ('none', 'null'):
        return []
    if s.startswith('[') and s.endswith(']') or (s.startswith('(') and s.endswith(')')):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, (list, tuple)):
                return [x for x in (str(x).strip() for x in parsed) if x]
        except Exception:
            pass
    return [x for x in (x.strip() for x in s.split(',')) if x]

def _normalize_csv_string(value, default: str='0') -> str:
    items = _parse_csv_list(value)
    if not items:
        return str(default)
    if len(items) == 1 and items[0] == '0':
        return '0'
    return ','.join(items)

class AuthorizationCache:

    def __init__(self, size: int=50):
        self.size = int(size)
        self.cache = {}

    def add(self, id_tag: str, id_tag_info: dict):
        if len(self.cache) >= self.size:
            try:
                del self.cache[next(iter(self.cache))]
            except Exception:
                self.cache = {}
        self.cache[str(id_tag)] = {'idTagInfo': id_tag_info, 'timestamp': convert_timeformat(datetime.now(timezone.utc).replace(tzinfo=timezone.utc))}

    def get(self, id_tag: str):
        return self.cache.get(str(id_tag))

    def clear(self):
        self.cache = {}

class LocalAuthorizationList:

    def __init__(self, max_length: int=1000, storage_path: str='local_auth_list.json', version_path: str='local_auth_list_version.txt'):
        self.max_length = int(max_length)
        self.storage_path = storage_path
        self.version_path = version_path
        self.snapshot_path = os.fspath(storage_path) + '.snapshot'
        self.identifiers = {}
        self.list_version = 0
        self._load_from_disk()

    def _load_from_disk(self):
        if os.path.exists(self.snapshot_path):
            try:
                with open(self.snapshot_path, 'r', encoding='utf-8') as stream:
                    snapshot = json.load(stream)
                version = snapshot['list_version']
                identifiers = snapshot['identifiers']
                if snapshot.get('format') != 'ocpp-local-auth-list-v1' or type(version) is not int or version < 0 or (not isinstance(identifiers, dict)):
                    raise ValueError('Invalid local authorization snapshot')
                self.list_version = version
                self.identifiers = identifiers
            except Exception:
                self.list_version = 0
                self.identifiers = {}
                logging.error('Local authorization snapshot could not be loaded')
            return
        try:
            if os.path.exists(self.version_path):
                with open(self.version_path, 'r') as f:
                    self.list_version = int(str(f.read()).strip() or '0')
        except Exception:
            self.list_version = 0
        try:
            if os.path.exists(self.storage_path):
                with open(self.storage_path, 'r') as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self.identifiers = data
        except Exception:
            self.identifiers = {}

    @staticmethod
    def _atomic_write(path, text):
        import tempfile
        target = os.path.abspath(os.fspath(path))
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, dir=os.path.dirname(target), prefix='.' + os.path.basename(target) + '.') as stream:
                temporary = stream.name
                stream.write(text)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
            temporary = None
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except FileNotFoundError:
                    pass

    def _persist(self, identifiers, list_version):
        snapshot = {'format': 'ocpp-local-auth-list-v1', 'list_version': list_version, 'identifiers': identifiers}
        serialized = json.dumps(snapshot, ensure_ascii=False, allow_nan=False)
        self._atomic_write(self.snapshot_path, serialized)
        for path, text in ((self.storage_path, json.dumps(identifiers, ensure_ascii=False)), (self.version_path, str(list_version))):
            try:
                self._atomic_write(path, text)
            except Exception:
                logging.warning('Local authorization compatibility mirror could not be updated')

    def _save_to_disk(self):
        self._persist(self.identifiers, int(self.list_version))

    def clear(self):
        self._persist({}, int(self.list_version))
        self.identifiers = {}

    def replace_list_full(self, new_entries: dict):
        if len(new_entries) > self.max_length:
            raise ValueError('LocalAuthList exceeds max_length')
        self.identifiers = new_entries

    def upsert(self, id_tag: str, id_tag_info: dict):
        if id_tag not in self.identifiers and len(self.identifiers) >= self.max_length:
            raise ValueError('LocalAuthList exceeds max_length')
        self.identifiers[str(id_tag)] = id_tag_info

    def delete(self, id_tag: str):
        self.identifiers.pop(str(id_tag), None)

    def apply_send_local_list(self, list_version, update_type, local_authorization_list):
        if isinstance(list_version, bool) or not isinstance(list_version, (int, str)):
            return 'Failed'
        try:
            lv = int(list_version)
        except Exception:
            return 'Failed'
        if not 0 <= lv <= 2147483647:
            return 'Failed'
        ut = str(getattr(update_type, 'value', update_type))
        if ut not in ('Full', 'Differential'):
            return 'Failed'
        if ut == 'Differential' and lv <= int(self.list_version):
            return 'VersionMismatch'
        if local_authorization_list is None:
            local_authorization_list = []
        if not isinstance(local_authorization_list, (list, tuple)):
            return 'Failed'
        if lv == 0 and local_authorization_list:
            return 'Failed'

        def getv(obj, camel, snake):
            if obj is None:
                return None
            if isinstance(obj, dict):
                return obj.get(snake) if snake in obj else obj.get(camel)
            return getattr(obj, snake, getattr(obj, camel, None))
        try:
            candidate = {} if ut == 'Full' else json.loads(json.dumps(self.identifiers))
            seen_tags = set()
            for item in local_authorization_list:
                id_tag = getv(item, 'idTag', 'id_tag')
                id_tag_info = getv(item, 'idTagInfo', 'id_tag_info')
                if not isinstance(id_tag, str) or not id_tag.strip() or len(id_tag) > 20:
                    return 'Failed'
                if id_tag in seen_tags:
                    return 'Failed'
                seen_tags.add(id_tag)
                if id_tag_info is None:
                    if ut == 'Full':
                        return 'Failed'
                    candidate.pop(id_tag, None)
                    continue
                if not isinstance(id_tag_info, dict):
                    try:
                        id_tag_info = dict(id_tag_info)
                    except Exception:
                        id_tag_info = vars(id_tag_info)
                info = json.loads(json.dumps(id_tag_info, allow_nan=False))
                status = str(getattr(id_tag_info.get('status'), 'value', id_tag_info.get('status')))
                if status not in ('Accepted', 'Blocked', 'Expired', 'Invalid', 'ConcurrentTx'):
                    return 'Failed'
                info['status'] = status
                expiry = getv(info, 'expiryDate', 'expiry_date')
                if expiry is not None:
                    if not isinstance(expiry, str):
                        return 'Failed'
                    parsed = datetime.fromisoformat(expiry.replace('Z', '+00:00'))
                    if parsed.tzinfo is None:
                        return 'Failed'
                parent = getv(info, 'parentIdTag', 'parent_id_tag')
                if parent is not None and (not isinstance(parent, str) or not parent or len(parent) > 20):
                    return 'Failed'
                candidate[id_tag] = info
            if len(candidate) > self.max_length:
                return 'Failed'
            self._persist(candidate, lv)
        except Exception:
            logging.error('Local authorization update could not be committed')
            return 'Failed'
        self.identifiers = candidate
        self.list_version = lv
        return 'Accepted'

class ChargePoint:

    def _autocharge_prefix(self):
        return getattr(self, 'AutoChargeIdTagPrefix_cfg', AUTOCHARGE_ID_TAG_PREFIX)

    async def on_get_configuration(self, **kwargs):
        full_configuration = [{'key': 'NumberOfConnectors', 'readonly': True, 'value': '2'}, {'key': 'SupportedFeatureProfiles', 'readonly': True, 'value': str(getattr(self, 'SupportedFeatureProfiles_cfg', 'Core,LocalAuthListManagement,RemoteTrigger,SmartCharging'))}, {'key': 'HeartbeatInterval', 'readonly': False, 'value': str(self.HeartbeatInterval_cfg)}, {'key': 'AuthorizationCacheEnabled', 'readonly': False, 'value': str(self.AuthorizationCacheEnabled_cfg)}, {'key': 'LocalAuthListEnabled', 'readonly': False, 'value': str(self.LocalAuthListEnabled_cfg)}, {'key': 'AutoChargeEnabled', 'readonly': False, 'value': str(self.AutoChargeEnabled_cfg)}, {'key': 'AutoChargeIdTagPrefix', 'readonly': False, 'value': str(self._autocharge_prefix())}, {'key': 'LocalAuthListMaxLength', 'readonly': True, 'value': str(self.LocalAuthListMaxLength_cfg)}, {'key': 'StopTxnSampledData', 'readonly': False, 'value': str(self.StopTxnSampledData_cfg)}, {'key': 'MeterValuesSampledData', 'readonly': False, 'value': str(self.MeterValuesSampledData_cfg)}, {'key': 'AllowOfflineTxForUnknownId', 'readonly': False, 'value': str(self.AllowOfflineTxForUnknownId_cfg)}, {'key': 'AuthorizeRemoteTxRequests', 'readonly': False, 'value': str(self.AuthorizeRemoteTxRequests_cfg)}, {'key': 'LocalAuthorizeOffline', 'readonly': False, 'value': str(self.LocalAuthorizeOffline_cfg)}, {'key': 'LocalPreAuthorize', 'readonly': False, 'value': str(self.LocalPreAuthorize_cfg)}, {'key': 'MeterValueSampleInterval', 'readonly': False, 'value': str(self.MeterValueSampleInterval_cfg)}, {'key': 'StopTransactionOnInvalidId', 'readonly': False, 'value': str(self.StopTransactionOnInvalidId_cfg)}, {'key': 'TransactionMessageAttempts', 'readonly': False, 'value': str(self.TransactionMessageAttempts_cfg)}, {'key': 'TransactionMessageRetryInterval', 'readonly': False, 'value': str(self.TransactionMessageRetryInterval_cfg)}, {'key': 'WebSocketPingInterval', 'readonly': False, 'value': str(self.WebSocketPingInterval_cfg)}]
        full_configuration.extend(stop_pin_config.configuration_items())
        for key, value in {'ChargeProfileMaxStackLevel': '100', 'ChargingScheduleAllowedChargingRateUnit': 'Current,Power', 'ChargingScheduleMaxPeriods': '256', 'MaxChargingProfilesInstalled': '128', 'ReserveConnectorZeroSupported': 'false', 'SupportedFileTransferProtocols': 'HTTP,HTTPS', 'SendLocalListMaxLength': str(self.LocalAuthListMaxLength_cfg), 'FirmwareUpdateMode': 'DownloadAndStage', 'GetConfigurationMaxKeys': '256'}.items():
            full_configuration.append({'key': key, 'readonly': True, 'value': value})
        if self._v2g_bridge is not None:
            full_configuration.extend(self._v2g_bridge.configuration_items())
        configuration_by_key = {item['key']: item for item in full_configuration}
        raw_keys = kwargs.get('key')
        if raw_keys is None:
            requested_keys = []
        elif isinstance(raw_keys, str):
            requested_keys = [raw_keys]
        else:
            try:
                requested_keys = list(raw_keys)
            except TypeError:
                requested_keys = [raw_keys]
        requested_keys = [str(key).strip() for key in requested_keys if key is not None and str(key).strip()]
        if not requested_keys:
            configuration_key = [dict(item) for item in full_configuration]
            unknown_key = []
        else:
            configuration_key = []
            unknown_key = []
            seen_keys = set()
            for key in requested_keys:
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                item = configuration_by_key.get(key)
                if item is None:
                    unknown_key.append(key)
                else:
                    configuration_key.append(dict(item))
        return call_result.GetConfiguration(configuration_key=configuration_key, unknown_key=unknown_key or None)

    async def on_change_configuration(self, key, value, **kwargs):
        if not isinstance(key, str):
            return call_result.ChangeConfiguration(status='Rejected')
        if key in STOP_PIN_KEYS:
            status = await asyncio.to_thread(stop_pin_config.change_configuration, key, value)
            return call_result.ChangeConfiguration(status=status)
        if key.startswith('V2G') or key in ('TimeToFullSampledInterval', 'TimeToFullSamapIedInterval', 'CurrentDemandSampledInterval'):
            status = self._v2g_bridge.change_configuration(key, value) if self._v2g_bridge is not None else 'NotSupported'
            return call_result.ChangeConfiguration(status=status)
        readonly_keys = {'LocalAuthListMaxLength', 'NumberOfConnectors', 'SupportedFeatureProfiles', 'ChargeProfileMaxStackLevel', 'ChargingScheduleAllowedChargingRateUnit', 'ChargingScheduleMaxPeriods', 'MaxChargingProfilesInstalled', 'ReserveConnectorZeroSupported', 'SupportedFileTransferProtocols', 'SendLocalListMaxLength', 'FirmwareUpdateMode', 'GetConfigurationMaxKeys'}
        if key in readonly_keys:
            return call_result.ChangeConfiguration(status='Rejected')
        configurable_keys = ['HeartbeatInterval', 'AuthorizationCacheEnabled', 'LocalAuthListEnabled', 'AutoChargeEnabled', 'AutoChargeIdTagPrefix', 'StopTxnSampledData', 'MeterValuesSampledData', 'AllowOfflineTxForUnknownId', 'AuthorizeRemoteTxRequests', 'LocalAuthorizeOffline', 'LocalPreAuthorize', 'MeterValueSampleInterval', 'StopTransactionOnInvalidId', 'TransactionMessageAttempts', 'TransactionMessageRetryInterval', 'WebSocketPingInterval']
        if key not in configurable_keys:
            return call_result.ChangeConfiguration(status='NotSupported')
        numeric_keys = {'HeartbeatInterval', 'MeterValueSampleInterval', 'TransactionMessageAttempts', 'TransactionMessageRetryInterval', 'WebSocketPingInterval'}
        boolean_keys = {'AuthorizationCacheEnabled', 'LocalAuthListEnabled', 'AutoChargeEnabled', 'AllowOfflineTxForUnknownId', 'AuthorizeRemoteTxRequests', 'LocalAuthorizeOffline', 'LocalPreAuthorize', 'StopTransactionOnInvalidId'}
        if key in numeric_keys:
            if isinstance(value, bool) or not isinstance(value, (str, int)):
                return call_result.ChangeConfiguration(status='Rejected')
            text = str(value).strip()
            if not text or any((char not in '0123456789' for char in text)):
                return call_result.ChangeConfiguration(status='Rejected')
            number = int(text)
            minimum = 1 if key == 'HeartbeatInterval' else 0
            if not minimum <= number <= 2147483647:
                return call_result.ChangeConfiguration(status='Rejected')
            normalized = str(number)
        elif key in boolean_keys:
            if not isinstance(value, str) or value.strip().lower() not in ('true', 'false'):
                return call_result.ChangeConfiguration(status='Rejected')
            normalized = value.strip().lower()
        elif key == 'AutoChargeIdTagPrefix':
            normalized = _normalize_autocharge_prefix(value)
            if normalized is None:
                return call_result.ChangeConfiguration(status='Rejected')
        else:
            normalized = _normalize_csv_string(value, default='0')
        try:
            _write_configuration_file(f'{key}_cfg.txt', normalized)
        except Exception:
            logging.error('OCPP configuration could not be persisted: %s', key)
            return call_result.ChangeConfiguration(status='Rejected')
        setattr(self, f'{key}_cfg', normalized)
        if key == 'AutoChargeEnabled' and normalized == 'true':
            try:
                self._seed_autocharge_events()
            except Exception:
                logging.exception('Could not seed retained AutoCharge events')
        return call_result.ChangeConfiguration(status='Accepted')

    async def on_clear_cache(self, **kwargs):
        try:
            self.authorization_cache.clear()
            logging.info('ClearCache: authorization cache cleared.')
            return call_result.ClearCache(status='Accepted')
        except Exception as e:
            logging.exception(f'ClearCache failed: {e}')
            return call_result.ClearCache(status='Rejected')

    async def on_get_local_list_version(self, **kwargs):
        try:
            lv = int(getattr(self.local_auth_list, 'list_version', 0) or 0)
            return call_result.GetLocalListVersion(list_version=lv)
        except Exception:
            return call_result.GetLocalListVersion(list_version=0)

    async def on_send_local_list(self, list_version=None, update_type=None, local_authorization_list=None, **kwargs):
        if list_version is None:
            list_version = kwargs.get('list_version')
        if update_type is None:
            update_type = kwargs.get('update_type')
        if local_authorization_list is None:
            local_authorization_list = kwargs.get('local_authorization_list')
        try:
            status = self.local_auth_list.apply_send_local_list(list_version, update_type, local_authorization_list)
            return call_result.SendLocalList(status=status)
        except Exception as e:
            logging.exception(f'SendLocalList failed: {e}')
            return call_result.SendLocalList(status='Failed')
