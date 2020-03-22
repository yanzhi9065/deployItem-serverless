import requests
import json

from flask import Flask, jsonify, request

from json_tools import decode_dict, encode_dict
#from mongo_utils import write_outfit

app = Flask(__name__)

def response_with_error(message):
    return jsonify({"message": message}), 500

def download_file_v2(url=""):
    timeout = 60
    res = requests.get(url, stream=True, timeout=timeout)
    raw_json = res.json()
    if raw_json:
        return json.dumps(raw_json).encode('utf-8')
    else:
        return None

def normalize_item(byts):
    """
    decode the item and check the instance type,
    if it's a dict, return ecnoded [dict]
    else return the input
    """
    instance = decode_dict(json.loads(byts.decode("utf-8")))
    if isinstance(instance, dict):
        return [instance]
    return instance

def compress_json_to_bytes(input):
    """
    zip json, to unzip file, use get_XXX_from_file
    """
    return json.dumps(encode_dict(input, backend="jpeg|zstd")).encode('utf-8')

@app.route('/')
def hello_world():
    return jsonify({"message": "Hello World!"})

@app.route('/deploy', methods=['POST'])
def deploy_item():
    payload = request.get_json(force=True)
    if payload is None:
        return response_with_error("payload is empty")
    uuid = payload.get("uuid", None)
    url = payload.get("url", None)
    version = payload.get("version", None)
    master_idx = payload.get("master_idx", None)
    if None in [uuid, url, master_idx, version]:
        return response_with_error("Input is not valid")

    byte_file = download_file_v2(url)
    if byte_file is None:
        return response_with_error("failed to download the file")

    # in case this is a fullbody and the json is a dict instead of list
    instance = normalize_item(byte_file)
    byte_file = compress_json_to_bytes(instance)

    # write_outfit(uuid, version, byte_file, master_idx)
    return jsonify({"message": "ok"})

if __name__ == '__main__':
    app.run()
