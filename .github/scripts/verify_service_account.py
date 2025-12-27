import os
import json
import base64


def detector(s: str) -> str:
    if not s:
        return "empty"
    try:
        json.loads(s)
        return "raw"
    except Exception:
        pass
    try:
        json.loads(s.replace('\\n', '\n'))
        return "escaped_newlines"
    except Exception:
        pass
    try:
        dec = base64.b64decode(s).decode('utf-8')
        json.loads(dec)
        return "base64"
    except Exception:
        pass
    return "invalid"


def main():
    s = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON', '')
    res = detector(s)
    print('DETECTOR_RESULT:', res)
    if res in ('raw', 'escaped_newlines', 'base64'):
        if res == 'base64':
            d = base64.b64decode(s).decode('utf-8')
        elif res == 'escaped_newlines':
            d = s.replace('\\n', '\n')
        else:
            d = s
        obj = json.loads(d)
        print('KEYS:', list(obj.keys()))
        pk = obj.get('private_key', '')
        print('private_key_length:', len(pk) if isinstance(pk, str) else 0)
    else:
        print('ERROR: formato inválido para GOOGLE_SERVICE_ACCOUNT_JSON')


if __name__ == '__main__':
    main()
