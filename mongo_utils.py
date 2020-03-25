import datetime
import gridfs

import os
import logging

import pymongo
from pymongo import MongoClient

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

MASTER_ITEM_IDX = 'master_item_idx'
MONGODB_URL = "mongodb://iris:xcBQvYSP3j8MNWGd@iris-itemdb-mongo.ccd6418lnpd7.us-west-2.docdb.amazonaws.com:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem",

class MongoDBUtils:
    def __init__(self):
        self._client = MongoClient(MONGODB_URL)
        self._db = self._client.formaIris

        # constant
        self._item_types = ['avatar', 'outfit']
        self._index_map = {
            'avatar': [
                ('uuid', 'uuid', False),
                (
                    'uuid_version',
                    [('uuid', pymongo.DESCENDING), ('version', pymongo.DESCENDING)],
                    True,
                ),
            ],
            'outfit': [
                ('uuid', 'uuid', False),
                (
                    'uuid_version',
                    [('uuid', pymongo.DESCENDING), ('version', pymongo.DESCENDING)],
                    True,
                ),
            ],
        }

        tables = self._db.collection_names()
        # check table and indexing
        for item_type in self._item_types:
            table_name = item_type + "s"
            if table_name not in tables:
                self._db.create_collection(table_name)

            table_head, _ = self._get_tables(item_type)

            current_index = table_head.index_information()

            for (index_name, index_cmd, is_unique) in self._index_map[item_type]:
                # check index, if no index, creat one
                logger.info("index name:{}, index_cmd:{}".format(index_name, index_cmd))
                if (index_name) not in current_index:
                    # debug
                    logger.info("index {0}_{1} missing, create".format(item_type, index_name))
                    table_head.create_index(index_cmd, name=index_name, unique=is_unique)

    def _create_record_template(self, type, uuid, version, opt):
        if type == "avatar":
            return {
                'uuid': uuid,
                'version': version,
                "createTime": datetime.datetime.now(),
                "fileID": None,
            }
        elif type == "outfit":
            master_item_idx = 0
            if 'master_item_idx' in opt:
                master_item_idx = opt['master_item_idx']
            return {
                'uuid': uuid,
                'version': version,
                'master_item_idx': master_item_idx,
                "createTime": datetime.datetime.now(),
                "fileID": None,
            }

    def _get_tables(self, item_type):
        assert type not in self._item_types, 'type has to be in {0}'.format(self._item_types)

        if item_type == "avatar":
            header_table = self._db.avatars
            file_table = gridfs.GridFS(self._db, collection="avatarFiles")
        elif item_type == "outfit":
            header_table = self._db.outfits
            file_table = gridfs.GridFS(self._db, collection="outfitFiles")
        else:
            logger.exception("unsupported tyep:" + item_type)

        return header_table, file_table

    def save_file(self, item_type, item_uuid, item_version, item_file, opt={}):
        header_table, file_table = self._get_tables(item_type)

        file_id = file_table.put(item_file)

        record = header_table.find_one({'uuid': item_uuid, 'version': item_version})
        if record is None:
            # record does ot exists, create a new one and insert it
            record = self._create_record_template(item_type, item_uuid, item_version, opt)
            record['fileID'] = file_id
            header_table.insert_one(record)
        else:
            update_dict = {}
            deprecate_file_id = None
            # record exists, rewrite it
            if record['fileID'] is not None:
                # delete old file record if it exists
                deprecate_file_id = record['fileID']

            update_dict['fileID'] = file_id

            if MASTER_ITEM_IDX in opt and \
                record[MASTER_ITEM_IDX] != opt[MASTER_ITEM_IDX]:
                update_dict[MASTER_ITEM_IDX] = opt[MASTER_ITEM_IDX]
            header_table.update_one({'_id': record['_id'],}, {'$set': update_dict})

            if deprecate_file_id is not None:
                file_table.delete(deprecate_file_id)

    def read_file(self, item_type, item_uuid, item_version):
        print("type:", item_type, "uuid:", item_uuid, "version:", item_version)
        print("version type:", type(item_version))
        header_table, file_table = self._get_tables(item_type)

        record = header_table.find_one({'uuid': item_uuid, 'version': item_version})
        print("try to find record")
        if record is not None:
            print("found record")
            record['file'] = file_table.get(record['fileID']).read()

        return record

    def read_file_without_version(self, item_type, uuid):
        header_table, file_table = self._get_tables(item_type)

        records = header_table.find({'uuid': uuid})

        max_version = -1
        record = None
        for rec in records:
            if rec['version'] > max_version:
                record = rec
                max_version = rec['version']

        print(max_version)
        if record is not None:
            record['file'] = file_table.get(record['fileID']).read()

        return record


# singleton
mongoDB = MongoDBUtils()

"""
  write outfit file to db
  note, If this is overwrite(uuid existed in db),
  set master_idx doesn't change anything.
"""


def write_outfit(uuid, version, file, master_idx=0):
    opt = {'master_item_idx': 0}

    if master_idx >= 0:
        opt['master_item_idx'] = master_idx

    return mongoDB.save_file("outfit", uuid, version, file, opt)


def read_outfit(uuid, version):
    if isinstance(version, int) and version >= 0:
        return mongoDB.read_file("outfit", uuid, version)
    else:
        return mongoDB.read_file_without_version("outfit", uuid)


def write_avatar(uuid, version, file):
    # For avatar, do not update version for now
    mongoDB.save_file("avatar", uuid, version, file)


def read_avatar(uuid, version):
    if isinstance(version, int) and version >= 0:
        return mongoDB.read_file("avatar", uuid, version)
    else:
        return mongoDB.read_file_without_version("avatar", uuid)


def get_master_idx(item_record):
    return 0 if 'master_item_idx' not in item_record else item_record['master_item_idx']


def get_file_from_record(record):
    if record is not None and 'file' in record:
        return record['file']
