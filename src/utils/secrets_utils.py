import secrets


def generate_nonce() -> str:
    return secrets.token_hex(16)
