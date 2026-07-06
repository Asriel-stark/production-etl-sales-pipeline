import base64
import sys
from Cryptodome.Cipher import AES
from Cryptodome.Protocol.KDF import PBKDF2
from resources.dev import config
from src.main.utility.logging_config import logger

try:
    key = config.key
    iv = config.iv
    salt = config.salt

    if not (key and iv and salt):
        raise ValueError("Encryption keys, IV, or Salt details are missing in configuration.")
except Exception as e:
    logger.error("Error occurred while fetching details for key/iv/salt. Details: %s", e)
    sys.exit(1)

BS = 16
pad = lambda s: bytes(s + (BS - len(s) % BS) * chr(BS - len(s) % BS), 'utf-8')
unpad = lambda s: s[0:-ord(s[-1:])]

def get_private_key():
    """Generates a 32-byte key using PBKDF2 key derivation function."""
    Salt = salt.encode('utf-8')
    kdf = PBKDF2(key, Salt, 64, 1000)
    key32 = kdf[:32]
    return key32

def encrypt(raw):
    """Encrypts raw text using AES CBC mode and returns base64 encoded bytes."""
    raw = pad(raw)
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    return base64.b64encode(cipher.encrypt(raw))

def decrypt(enc):
    """Decrypts base64 encoded string using AES CBC mode and returns decrypted utf-8 string."""
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    return unpad(cipher.decrypt(base64.b64decode(enc))).decode('utf8')