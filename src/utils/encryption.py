import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from typing import Optional

def generate_key_pair():
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    public_key = private_key.public_key()
    
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    
    return {
        'private_key': base64.b64encode(private_pem).decode('utf-8'),
        'public_key': base64.b64encode(public_pem).decode('utf-8')
    }

def encrypt_token(token: str, public_key_pem: str) -> str:
    public_key_bytes = base64.b64decode(public_key_pem.encode('utf-8'))
    public_key = serialization.load_pem_public_key(
        public_key_bytes,
        backend=default_backend()
    )
    
    encrypted = public_key.encrypt(
        token.encode('utf-8'),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    
    return base64.b64encode(encrypted).decode('utf-8')

def decrypt_token(encrypted_token: str, private_key_pem: str) -> str:
    private_key_bytes = base64.b64decode(private_key_pem.encode('utf-8'))
    private_key = serialization.load_pem_private_key(
        private_key_bytes,
        password=None,
        backend=default_backend()
    )
    
    encrypted_bytes = base64.b64decode(encrypted_token.encode('utf-8'))
    
    decrypted = private_key.decrypt(
        encrypted_bytes,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    
    return decrypted.decode('utf-8')

def validate_public_key(public_key_pem: str) -> bool:
    try:
        public_key_bytes = base64.b64decode(public_key_pem.encode('utf-8'))
        serialization.load_pem_public_key(
            public_key_bytes,
            backend=default_backend()
        )
        return True
    except Exception:
        return False

