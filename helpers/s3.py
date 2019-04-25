import io
import os
import logging
import boto3
from botocore.client import Config
from tqdm import tqdm
from helpers import misc

AWS_ACCESS_KEY_ID = '***REMOVED***'
AWS_SECRET_ACCESS_KEY = '***REMOVED***'
BUCKET = 'tcm-user201'

_log = logging.getLogger(__name__)


# I am (Sergey) Still working on that module, that's why here such many prints and a lot of hardcoded stuff

def get_s3_source(aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY):
    s3 = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)
    return s3


def get_bucket(s3=get_s3_source(), bucket=BUCKET):
    return s3.Bucket(bucket)


def get_client(aws_access_key_id=AWS_ACCESS_KEY_ID,
               aws_secret_access_key=AWS_SECRET_ACCESS_KEY):
    c = boto3.client(
        's3',
        config=Config(signature_version='s3v4'),
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)
    return c


def get_total(file_path):
    """
    Return rounded file size in Mb
    :param file_path: str
    :return: float
    """
    size = float(os.stat(file_path).st_size) / 1024 / 1024
    return round(size, 3)


def upload_file(local_path, remote_path, verbose=True):
    s3 = get_s3_source()
    total = get_total(local_path)
    with tqdm(
            total=total,
            desc='Current file(MB)',
            unit='MB',
            disable=not verbose) as pbar:
        s3.Object(BUCKET, remote_path).upload_fileobj(
            open(local_path, 'rb'),
            Callback=lambda x: pbar.update(round(float(x) / 1024 / 1024, 3)))


def upload_folder(local_folder, remote_folder, fake=False):
    if not local_folder.endswith('/'):
        local_folder += '/'
    for root, folders, files in os.walk(local_folder):
        for f in tqdm(files, desc='Total in folder {}'.format(root)):
            local_file = os.path.join(root, f)
            remote_file = os.path.join(remote_folder,
                                       root.split(local_folder)[1], f)
            if fake:
                print(('from:', local_file))
                print(('to:  ', remote_file))
            else:
                upload_file(local_file, remote_file)


def generate_url(file_path):
    cl = get_client()
    url = cl.generate_presigned_url(
        ClientMethod='get_object', Params={
            'Bucket': BUCKET,
            'Key': file_path
        })
    return url


def list_dirs(prefix=''):
    cl = get_client()
    rf = cl.list_objects(Bucket=BUCKET, Prefix=prefix, Delimiter='/')
    if rf.get('CommonPrefixes'):

        if prefix:
            return [str(i['Prefix']).split(prefix)[-1] for i in rf.get('CommonPrefixes')]
        else:
            return [str(i['Prefix']) for i in rf.get('CommonPrefixes')]
    return []


def list_files(prefix='', bucket=BUCKET):
    cl = get_client()
    rf = cl.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
    if rf.get('Contents'):
        if prefix:
            return [str(i['Key']).split(prefix)[-1] for i in rf.get('Contents')]
        else:
            return [str(i['Key']) for i in rf.get('Contents')]
    return []


def show_dir(prefix='', to_print=True):
    dirs = sorted(list_dirs(prefix))
    files = sorted(list_files(prefix))
    dirs.extend(files)
    if to_print:
        for i in dirs:
            print(i)
    return dirs


def get_all_keys(prefix='', bucket=BUCKET):
    b = get_bucket(bucket=bucket)
    keys = [str(i._key) for i in tqdm(b.objects.filter(Prefix=prefix))]
    return keys


def keys_to_objects(keys):
    objects = [{'Key': i} for i in keys]

    return {'Objects': objects}


def download_file(remote_file, local_path, verbose=True):
    bucket = get_bucket()
    file_name = os.path.basename(remote_file)
    remote_file = remote_file.split('s3://')[-1]
    if not local_path:
        dst_path = os.path.join('./', file_name)
    else:
        dst_path = os.path.join(local_path, file_name)
    misc.check_or_create_dir(os.path.dirname(dst_path))
    _log.info('Downloading file:\n{} -> {}'.format(remote_file, dst_path))
    obj = bucket.Object(remote_file)
    size = obj.content_length
    with tqdm(
            total=float(size) / 1024 / 1024, desc='Current file(MB)',
            unit='MB',
            disable=not verbose) as pbar:
        bucket.download_file(
            remote_file,
            dst_path,
            Callback=lambda x: pbar.update(float(x) / 1024 / 1024))


def download_folder(from_folder_name, to_folder_name, fake=False):
    files = list_files(prefix=from_folder_name)

    for f in tqdm(files, desc='Files '):
        if f.split(from_folder_name)[-1] and not f.endswith('/'):
            s = f.split(from_folder_name)[-1]
            if s.startswith('/'):
                s = s[1:]
            output_file_name = os.path.join(to_folder_name, s)
            out_dir = os.path.join(to_folder_name,
                                   os.path.dirname(output_file_name))
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
            if fake:
                print(('from: {}\nto: {}\n'.format(f, output_file_name)))
            else:
                download_file(f, output_file_name)


def delete_object(keys):
    b = get_bucket()
    objects = keys_to_objects(keys)
    return b.delete_objects(Delete=objects)


def get_object(remote_file):
    bucket = get_bucket()
    obj = bucket.Object(remote_file)
    return obj


def hook(t):
    def inner(bytes_amount):
        t.update(bytes_amount)

    return inner


def get_file_buffer(remote_file):
    obj = get_object(remote_file)
    buf = io.BytesIO()
    with tqdm(total=obj.content_length, unit='B', unit_scale=True, desc="Downloading {}".format(remote_file)) as t:
        obj.download_fileobj(buf, Callback=hook(t))
    buf.seek(0)
    return buf


def get_metadata(remote_file):
    return get_object(remote_file).metadata
