
def retrieve_objects_list(s3_client, bucket_name, prefix=None, delimiter=None):
    """
    This method takes input as s3_client,bucket_name,prefix,delimiter and return object names in the form of list

    :param s3_client    :   Represents s3 client
    :param bucket_name  :   Represents bucket name
    :param prefix       :   Represents prefix
    :param delimiter    :   Represents delimiter
    :return             :   return list of object names
    """

    prefix = prefix if prefix else ""
    response = objects_list_with_prefix(s3_client, bucket_name, prefix, delimiter)
    response_objects_list = []
    if "CommonPrefixes" in response:
        response_objects_list = response_objects_list + response["CommonPrefixes"]
    if "Contents" in response:
        response_objects_list = response_objects_list + response["Contents"]
    while "NextContinuationToken" in response and response["NextContinuationToken"]:
        response = objects_list_with_prefix_next_token(s3_client, bucket_name, prefix,response["NextContinuationToken"], delimiter)
        if "CommonPrefixes" in response:
            response_objects_list = response_objects_list + response["CommonPrefixes"]
        if "Contents" in response:
            response_objects_list = response_objects_list + response["Contents"]
    return response_objects_list


def objects_list_with_prefix(s3_client, bucket_name, prefix, delimiter):
    """
    This method takes input as s3_client,bucket_name,prefix,delimiter and return objects with prefix in the form of list
    :param s3_client    :   Represents s3 client
    :param bucket_name  :   Represents bucket name
    :param prefix       :   Represents prefix
    :param delimiter    :   Represents delimiter
    :return             :   Return objects along with prefix
    """

    prefix = prefix if prefix else ""
    delimiter = delimiter if delimiter else ""
    list_s3_objects = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1000,
                                                Prefix=prefix, FetchOwner=True, Delimiter=delimiter)
    return list_s3_objects


def objects_list_with_prefix_next_token(s3_client, bucket_name, prefix, continuation_token, delimiter=None):

    """
    This method takes input as s3_client,bucket_name,prefix,delimiter ,continuation_token and return objects with prefix in the form of list
    :param s3_client            :   Represents s3 client
    :param bucket_name          :   Represents bucket name
    :param prefix               :   Represents with prefix
    :param continuation_token   :   Represents next token
    :param delimiter            :   Represents delimiter
    :return                     :   Returns objects list based on token
    """
    prefix = prefix if prefix else ""
    delimiter = delimiter if delimiter else ""
    list_s3_objects = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1000,
                                                Prefix=prefix, FetchOwner=True,
                                                ContinuationToken=continuation_token, Delimiter=delimiter)
    return list_s3_objects


def retrieve_object(s3_client, bucket_name, key):

    """
    This method takes input as s3_client,bucket_name,object_key and return object data
    :param s3_client    :   Represents s3 client
    :param bucket_name  :   Represents bucket_name
    :param key          :   Represents object key
    :return             :   returns object data in the form of list
    """
    obj = s3_client.Object(bucket_name, key)
    obj_data = obj.get()['Body'].read().splitlines()
    return obj_data