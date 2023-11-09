import datetime

def generate_temporary_public_url(client, bucket_name, file_path, timeout_seconds):
    # Get the bucket and blob
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    # Calculate the expiration time
    expiration_time = datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout_seconds)

    # Generate a signed URL that expires after the specified timeout
    signed_url = blob.generate_signed_url(expiration=expiration_time)

    return signed_url
