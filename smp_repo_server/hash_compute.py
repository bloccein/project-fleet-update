import struct, hashlib
import sys

def compute_hash(blob: bytes) -> str:
    """Compute SHA-256 over hdr_sz + img_sz + prot_sz bytes from the given file bytes."""

    if len(blob) < 32:
        raise ValueError("data too short for header")

    magic, load, hdr_sz, prot_sz, img_sz = struct.unpack_from("<IIHHI", blob, 0)

    if magic != 0x96f3b83d:
        raise ValueError(f"bad magic: {hex(magic)}")

    hashed_len = hdr_sz + img_sz + prot_sz
    if len(blob) < hashed_len:
        raise ValueError("data too short for hashed length")

    return hashlib.sha256(blob[:hashed_len]).hexdigest()
