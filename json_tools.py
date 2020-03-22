"""
This module encodes and decodes raw buffer to json strings (json_tools.py)
"""

import logging
import io
import base64
import json

import zlib
import zstd
import lz4.frame

import numpy as np
import cv2


def encode_image(image, ext='.png', quality=95, add_header=False):
    """encodes a numpy array image into a json string, compressed with cv2"""
    if image.size == 0 or image.dtype != np.uint8:
        raise ValueError("Incorrect image format. " "Use encode_numpy_array() instead")
    flags = []

    if ext == '.jpg':
        flags = [cv2.IMWRITE_JPEG_QUALITY, quality]
    elif ext == '.webp':
        flags = [cv2.IMWRITE_WEBP_QUALITY, quality]

    image_str = cv2.imencode(ext, image, flags)[1].tostring()
    image_encoded = base64.b64encode(image_str).decode("utf-8")

    header = 'data:image/%s;base64,' % ext[1:] if add_header else ''
    return header + image_encoded


def decode_image(string):
    """decodes a json string and return a numpy array image"""
    header_pos = string.find(';base64,', 0, 30)
    stripped = string
    if header_pos > 0:
        stripped = string[header_pos + 8 :]
    data = bytearray(base64.b64decode(stripped.encode("utf-8")))
    image = cv2.imdecode(np.array(data), -1)
    return image


def encode_numpy_array(arr, backend="zstd"):
    """encodes a numpy array into a json string, compressed with backend"""
    # pylint: disable-msg=no-member
    buf = io.BytesIO()
    np.save(buf, arr)
    if backend == "lz4":
        compressed = lz4.frame.compress(buf.getvalue())
    elif backend == "zlib":
        compressed = zlib.compress(buf.getvalue())
    else:
        compressed = zstd.compress(buf.getvalue())
    return base64.b64encode(compressed).decode("utf-8")


def decode_numpy_array(string, backend="zstd"):
    """decodes a json string and return a numpy array"""
    # pylint: disable-msg=no-member
    compressed = base64.b64decode(string.encode("utf-8"))
    if backend == "lz4":
        decompressed = lz4.frame.decompress(compressed)
    elif backend == "zlib":
        decompressed = zlib.decompress(compressed)
    else:
        decompressed = zstd.decompress(compressed)
    return np.load(io.BytesIO(decompressed), allow_pickle=True)


def encode_dict(dct, backend="zstd"):
    """
    encodes a dict or a list of dict to a json-dumpable object

    Under the hood, this function adds a postfix to the key (after converting
    it to str) in the form of "|<name-type>|<value-type>", and calls other
    encode functions to encode (and compress) data.

    Supported dict key types:
        string (default)
        int

    Supported dict value types:
        all native types that are compatiable with json.dumps (default)
        dict
        numpy.ndarray
        numpy.ndarray.lz4 (numpy.ndarray when use "lz4" backend for compression)
        numpy.ndarray.zstd (use "zstd" backend for compression)
    """
    if isinstance(dct, list):
        encoded_dct = []
        for single_dct in dct:
            encoded_dct.append(encode_dict(single_dct, backend=backend))
    elif isinstance(dct, dict):
        encoded_dct = {}
        for key, value in dct.items():
            # process key
            if isinstance(key, int):
                new_key = str(key) + '|int'
            else:
                new_key = key + '|'

            # process value
            if isinstance(value, list):
                encoded_dct[new_key + "|list"] = encode_dict(value, backend=backend)
            elif isinstance(value, dict):
                encoded_dct[new_key + "|dict"] = encode_dict(value, backend=backend)
            elif isinstance(value, np.ndarray):
                use_jpeg = 'jpeg' in backend
                dim_valid = value.ndim >= 2
                if use_jpeg and dim_valid and np.min(value.shape[:2]) > 100:
                    use_float = value.dtype != np.uint8
                    if use_float:
                        value = np.clip(value * 255, 0, 255).astype(np.uint8)

                    field = new_key + "|jpeg"
                    if use_float:
                        field += '.float'

                    if value.ndim == 2:
                        value = value[:, :, np.newaxis]

                    if value.shape[2] >= 3:
                        bgr = value[:, :, :3]
                        encoded = encode_image(bgr, '.jpg')
                        remaining = None
                        if value.shape[2] > 3:
                            remaining = encode_numpy_array(value[:, :, 3:], backend="zstd")
                        encoded = (encoded, remaining)
                    else:
                        encoded = encode_numpy_array(value, backend="zstd")
                    encoded_dct[field] = encoded
                    continue
                if "lz4" in backend:
                    encoded_dct[new_key + "|numpy.ndarray.lz4"] = encode_numpy_array(
                        value, backend="lz4"
                    )
                elif "zstd" in backend:
                    encoded_dct[new_key + "|numpy.ndarray.zstd"] = encode_numpy_array(
                        value, backend="zstd"
                    )
                else:
                    encoded_dct[new_key + "|numpy.ndarray"] = encode_numpy_array(
                        value, backend="zlib"
                    )
            else:
                encoded_dct[new_key + "|"] = value
    else:
        encoded_dct = dct
    return encoded_dct


def decode_dict(dct):
    """
    decodes a json-parsed object to the original data

    This is the dual function of encode_dict(). The input can be a dict or
    a list of dict.

    The dict must have string as keys. Each key is ended with
    "|<name-type>|<value-type>".

    Supported dict key types:
        string (default)
        int

    Supported dict value types:
        all native types that are compatiable with json.dumps (default)
        dict
        numpy.ndarray
        numpy.ndarray.lz4 (numpy.ndarray when use "lz4" backend for compression)
        numpy.ndarray.zstd (use "zstd" backend for compression)
    """
    if isinstance(dct, list):
        decoded_dct = []
        for single_dct in dct:
            decoded_dct.append(decode_dict(single_dct))
    elif isinstance(dct, dict):
        decoded_dct = {}
        for key, value in dct.items():
            key_splits = key.rsplit('|', 2)
            # process key
            if key_splits[1] == 'int':
                key_name = int(key_splits[0])
            else:
                key_name = key_splits[0]
            # process value
            if key_splits[2] == "list":
                decoded_dct[key_name] = decode_dict(value)
            elif key_splits[2] == "dict":
                decoded_dct[key_name] = decode_dict(value)
            elif key_splits[2] == "numpy.ndarray":
                decoded_dct[key_name] = decode_numpy_array(value, backend="zlib")
            elif key_splits[2] == "numpy.ndarray.lz4":
                decoded_dct[key_name] = decode_numpy_array(value, backend="lz4")
            elif key_splits[2] == "numpy.ndarray.zstd":
                decoded_dct[key_name] = decode_numpy_array(value, backend="zstd")
            elif key_splits[2].startswith("jpeg"):
                use_float = key_splits[2].endswith("float")
                if isinstance(value, (list, tuple)):
                    bgr, alpha = value
                    full = decode_image(bgr)
                    if alpha is not None:
                        alpha = decode_numpy_array(alpha, backend='zstd')
                        full = np.dstack((full, alpha))
                else:
                    full = decode_numpy_array(value, backend='zstd')
                if use_float:
                    full = full.astype(np.float32) / 255
                decoded_dct[key_name] = np.squeeze(full)
            else:
                decoded_dct[key_name] = value
    else:
        decoded_dct = dct
    return decoded_dct


def save_dict_to_json(dct, jsonfile, backend="zstd"):
    """
    encodes a dict and save it to a file
    """
    with open(jsonfile, "w", encoding="utf-8") as filehandle:
        json.dump(encode_dict(dct, backend), filehandle, indent=4)


def load_dict_from_json(jsonfile):
    """
    load a json file and decode it
    """
    with open(jsonfile, "r", encoding="utf-8") as filehandle:
        return decode_dict(json.load(filehandle))


def load_json_from_url(url):
    """load json from url and decode it"""
    import urllib.request
    import json

    with urllib.request.urlopen(url) as url_obj:
        return decode_dict(json.loads(url_obj.read().decode()))
