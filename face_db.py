import lmdb
import json
import cv2
import nats
import asyncio
import numpy as np
from PIL import Image

# Open (and create if necessary) our database environment. Must specify
# max_dbs=... since we're opening subdbs.
env = lmdb.open('/home/nivetheni/lmdb/face-detection.lmdb',
                max_dbs=10, map_size=int(1e9))

# Now create subdbs for home and business addresses.
whitelist_db = env.open_db(b'whitelist')
blacklist_db = env.open_db(b'blacklist')
unknown_db = env.open_db(b'unknown')
visitor_db = env.open_db(b'visitor')

# Function to convert Numpy array to list
class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """

    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32,
                              np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


pic_arr = cv2.imread('/home/nivetheni/lmdb/selected/wajoud/0070.jpg')

face_det = {}
face_det['id'] = "Wajoud_7"
face_det['type'] = "visitor"
face_det['numpy_arr'] = pic_arr

face_det_id = json.dumps(face_det['id'])
face_det_str = json.dumps(face_det, cls=NumpyEncoder)
face_id_bytes = bytearray(face_det_id, "utf-8")
face_det_bytes = bytearray(face_det_str, "utf-8")


async def push_db():
    # Adding to respective DB:
    with env.begin(write=True) as txn:
        if (face_det['type'] == "whitelist"):
            txn.put(face_id_bytes, face_det_bytes, db=whitelist_db)
        elif (face_det['type'] == "blacklist"):
            txn.put(face_id_bytes, face_det_bytes, db=blacklist_db)
        elif (face_det['type'] == "unknown"):
            txn.put(face_id_bytes, face_det_bytes, db=unknown_db)
        elif (face_det['type'] == "visitor"):
            txn.put(face_id_bytes, face_det_bytes, db=visitor_db)

async def show_db():
    # Iterate each DB to show the keys are sorted:
    with env.begin() as txn:
        for name, db in ('whitelist', whitelist_db), ('blacklist', blacklist_db):
            print('DB:', name)
            for key, value in txn.cursor(db=db):
                print('  ', key, value)
        print()

async def update_db():
    # Update the type of a person
    with env.begin(write=True, db=whitelist_db) as txn:
        print('Updating the type of a person')
        txn.put()

        txn.delete()
        print()
        
        for key, value in txn.cursor():
            print('  ', key, value)
        print()

async def main():

    # ipfs

    # Push to db
    await push_db()

    # On updation
    await update_db()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete()
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
        loop.run_forever()
    except RuntimeError as e:
        print("error ", e)
