"""CSMS PIN configuration stored on the charger; never broadcast to HMI."""
import hmac
import logging
import os
from pathlib import Path
import threading

# Internal slot 0 represents one station-wide PIN, independent of request head.
KEYS = {'AutoChargeStopPin': 0}


def valid_config_pin(value):
    return isinstance(value, str) and (value == '' or
        (1 <= len(value) <= 10 and all('0' <= c <= '9' for c in value)))


class StopPinConfig:
    def __init__(self, directory):
        self.directory = Path(directory)
        self._lock = threading.RLock()
        self._pins = {}
        self._versions = {0: 0}
        for key, head in KEYS.items():
            try:
                pin = self._path(head).read_text(encoding='utf-8')
            except FileNotFoundError:
                pin = ''
            except (OSError, UnicodeError):
                logging.warning('[STOP-PIN-CONFIG] Cannot read shared PIN; disabled')
                pin = ''
            if not valid_config_pin(pin):
                logging.warning('[STOP-PIN-CONFIG] Invalid saved shared PIN; disabled')
                pin = ''
            self._pins[head] = pin

    def _path(self, head):
        return self.directory / 'StopPin_cfg.txt'

    def configuration_items(self):
        with self._lock:
            return [dict(key=key, readonly=False, value=self._pins[head]) for key, head in KEYS.items()]

    def change_configuration(self, key, value):
        if key not in KEYS or not valid_config_pin(value):
            return 'Rejected'
        head = KEYS[key]
        with self._lock:
            path = self._path(head)
            temporary = path.with_suffix('.tmp')
            try:
                with temporary.open('w', encoding='utf-8', newline='') as handle:
                    handle.write(value)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, path)
            except OSError:
                logging.warning('[STOP-PIN-CONFIG] Cannot persist shared PIN; rejected')
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    pass
                return 'Rejected'
            self._pins[head] = value
            self._versions[head] += 1
        logging.info('[STOP-PIN-CONFIG] Shared PIN updated')
        return 'Accepted'

    def check(self, head, password):
        with self._lock:
            expected = self._pins.get(0, '')
            matched = type(head) is int and head in (1, 2) and bool(expected) and isinstance(password, str) and password.isascii() and hmac.compare_digest(expected, password)
            return matched, self._versions[0], bool(expected)

    def unchanged(self, head, version):
        with self._lock:
            return type(head) is int and head in (1, 2) and self._versions[0] == version
