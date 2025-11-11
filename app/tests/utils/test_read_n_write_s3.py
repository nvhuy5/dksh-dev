import io
import json
from models.class_models import StatusEnum
import pytest
from unittest.mock import MagicMock
from botocore.exceptions import ClientError, BotoCoreError

from fastapi_celery.utils import read_n_write_s3 as s3_utils


# === Fixtures ===
@pytest.fixture(autouse=True)
def clear_connectors():
    """Automatically clear cached S3 connectors before each test."""
    s3_utils._s3_connectors.clear()
    yield
    s3_utils._s3_connectors.clear()


@pytest.fixture
def mock_client():
    """Return a mock S3 client."""
    return MagicMock()


@pytest.fixture
def bucket_and_key():
    """Provide a reusable bucket and key."""
    return "test-bucket", "test.json"


# === put_object ===
def test_put_object_with_buffer_success(mock_client, bucket_and_key):
    """Should upload BytesIO successfully."""
    buf = io.BytesIO(b"data")
    result = s3_utils.put_object(mock_client, *bucket_and_key, buf)
    mock_client.upload_fileobj.assert_called_once()
    assert result["status"] == StatusEnum.SUCCESS


def test_put_object_with_filepath_success(mock_client, bucket_and_key):
    """Should upload local file successfully."""
    result = s3_utils.put_object(mock_client, *bucket_and_key, "dummy.txt")
    mock_client.upload_file.assert_called_once()
    assert result["status"] == StatusEnum.SUCCESS


def test_put_object_with_invalid_type(mock_client, bucket_and_key):
    """Should fail when uploading_data is invalid type."""
    result = s3_utils.put_object(mock_client, *bucket_and_key, 123)
    assert result["status"] == StatusEnum.FAILED
    assert "uploading data" in result["error"]


def test_put_object_with_client_error(mock_client, bucket_and_key):
    """Should handle ClientError gracefully."""
    mock_client.upload_fileobj.side_effect = ClientError(
        {"Error": {"Code": "500", "Message": "Failed"}}, "upload"
    )
    buf = io.BytesIO(b"data")
    result = s3_utils.put_object(mock_client, *bucket_and_key, buf)
    assert result["status"] == StatusEnum.FAILED


# === get_object ===
def test_get_object_success(mock_client, bucket_and_key):
    """Should return BytesIO when object fetched successfully."""
    mock_client.get_object.return_value = {"Body": io.BytesIO(b"content")}
    buf = s3_utils.get_object(mock_client, *bucket_and_key)
    assert isinstance(buf, io.BytesIO)
    assert buf.read() == b"content"


def test_get_object_fail(mock_client, bucket_and_key):
    """Should return None when ClientError occurs."""
    mock_client.get_object.side_effect = ClientError({"Error": {}}, "get_object")
    buf = s3_utils.get_object(mock_client, *bucket_and_key)
    assert buf is None


# === copy_object_between_buckets ===
def test_copy_object_between_buckets_success(mocker):
    """Should copy successfully between buckets."""
    mock_client = MagicMock()
    mocker.patch.object(s3_utils.aws_connection, "S3Connector", return_value=MagicMock(client=mock_client))
    result = s3_utils.copy_object_between_buckets("src-bucket", "src-key", "dest-bucket", "dest-key")
    assert result["status"] == StatusEnum.SUCCESS
    mock_client.copy_object.assert_called_once()


def test_copy_object_between_buckets_fail(mocker):
    """Should return Failed when copy_object raises error."""
    mock_client = MagicMock()
    mock_client.copy_object.side_effect = ClientError({"Error": {}}, "copy_object")
    mocker.patch.object(s3_utils.aws_connection, "S3Connector", return_value=MagicMock(client=mock_client))
    result = s3_utils.copy_object_between_buckets("src", "key", "dest", "key2")
    assert result["status"] == StatusEnum.FAILED


# === object_exists ===
def test_object_exists_true(mock_client, bucket_and_key):
    """Should return True and metadata when object exists."""
    mock_client.head_object.return_value = {"ContentLength": 100}
    exists, meta = s3_utils.object_exists(mock_client, *bucket_and_key)
    assert exists is True
    assert meta["ContentLength"] == 100


def test_object_exists_not_found(mock_client, bucket_and_key):
    """Should return False when object not found."""
    error = ClientError({"Error": {"Code": "404"}}, "head_object")
    mock_client.head_object.side_effect = error
    exists, meta = s3_utils.object_exists(mock_client, *bucket_and_key)
    assert not exists
    assert meta is None


def test_object_exists_other_error(mock_client, bucket_and_key):
    """Should return False when unexpected error occurs."""
    mock_client.head_object.side_effect = ClientError({"Error": {"Code": "500"}}, "head_object")
    exists, meta = s3_utils.object_exists(mock_client, *bucket_and_key)
    assert not exists
    assert meta is None


# === any_json_in_s3_prefix ===
def test_any_json_in_s3_prefix_found(mocker):
    """Should return True when JSON file exists."""
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": [{"Key": "data/file.json"}]}]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = paginator
    mocker.patch.object(s3_utils.aws_connection, "S3Connector", return_value=MagicMock(client=mock_client))

    result = s3_utils.any_json_in_s3_prefix("bucket", "prefix")
    assert result is True


def test_any_json_in_s3_prefix_not_found(mocker):
    """Should return False when no JSON file found."""
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": [{"Key": "file.txt"}]}]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = paginator
    mocker.patch.object(s3_utils.aws_connection, "S3Connector", return_value=MagicMock(client=mock_client))

    result = s3_utils.any_json_in_s3_prefix("bucket", "prefix")
    assert result is False


# === write_json_to_s3 ===
def test_write_json_to_s3_success(mocker, mock_client):
    """Should upload JSON successfully."""
    mocker.patch.object(s3_utils, "put_object", return_value={"status": "Success"})
    mocker.patch.object(s3_utils.aws_connection, "S3Connector", return_value=MagicMock(client=mock_client, bucket_name="bucket"))

    data = {"foo": "bar"}
    result = s3_utils.write_json_to_s3(data, "bucket", "key.json")
    assert result["status"] == "Success"


def test_write_json_to_s3_fail_on_upload(mocker, mock_client):
    """Should return Failed when upload fails."""
    mocker.patch.object(s3_utils, "put_object", return_value={"status": StatusEnum.FAILED, "error": "oops"})
    mocker.patch.object(s3_utils.aws_connection, "S3Connector", return_value=MagicMock(client=mock_client, bucket_name="bucket"))
    result = s3_utils.write_json_to_s3({"a": 1}, "bucket", "key.json")
    assert result["status"] == "Failed"
    assert "error" in result


def test_write_json_to_s3_exception(mocker, mock_client):
    """Should catch unexpected exception and return Failed."""
    mocker.patch.object(s3_utils, "put_object", side_effect=Exception("crash"))
    mocker.patch.object(s3_utils.aws_connection, "S3Connector", return_value=MagicMock(client=mock_client, bucket_name="bucket"))
    result = s3_utils.write_json_to_s3({"a": 1}, "bucket", "key.json")
    assert result["status"] == "Failed"
    assert "crash" in result["error"]


# === read_json_from_s3 ===
def test_read_json_from_s3_success(mocker, mock_client):
    """Should parse JSON successfully."""
    buffer = io.BytesIO(json.dumps({"a": 1}).encode())
    mocker.patch.object(s3_utils, "get_object", return_value=buffer)
    mocker.patch.object(s3_utils.aws_connection, "S3Connector", return_value=MagicMock(client=mock_client))
    result = s3_utils.read_json_from_s3("bucket", "key.json")
    assert result == {"a": 1}


def test_read_json_from_s3_none(mocker, mock_client):
    """Should return None when no buffer returned."""
    mocker.patch.object(s3_utils, "get_object", return_value=None)
    mocker.patch.object(s3_utils.aws_connection, "S3Connector", return_value=MagicMock(client=mock_client))
    result = s3_utils.read_json_from_s3("bucket", "key.json")
    assert result is None


def test_read_json_from_s3_invalid_json(mocker, mock_client):
    """Should return None when JSON type is not dict."""
    buffer = io.BytesIO(json.dumps([1, 2, 3]).encode())
    mocker.patch.object(s3_utils, "get_object", return_value=buffer)
    mock_logger = mocker.patch.object(s3_utils, "logger")
    mocker.patch.object(s3_utils.aws_connection, "S3Connector", return_value=MagicMock(client=mock_client))
    result = s3_utils.read_json_from_s3("bucket", "key.json")
    assert result is None
    mock_logger.warning.assert_called_once()


def test_read_json_from_s3_exception(mocker, mock_client):
    """Should handle unexpected error."""
    mocker.patch.object(s3_utils, "get_object", side_effect=Exception("boom"))
    mock_logger = mocker.patch.object(s3_utils, "logger")
    mocker.patch.object(s3_utils.aws_connection, "S3Connector", return_value=MagicMock(client=mock_client))
    result = s3_utils.read_json_from_s3("bucket", "key.json")
    assert result is None
    mock_logger.error.assert_called_once()


# === list_objects_with_prefix ===
def test_list_objects_with_prefix_success(mocker):
    """Should return all keys under prefix."""
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": [{"Key": "a.json"}, {"Key": "b.json"}]}]
    mock_client = MagicMock()
    mock_client.get_paginator.return_value = paginator
    mocker.patch.object(s3_utils.aws_connection, "S3Connector", return_value=MagicMock(client=mock_client))

    result = s3_utils.list_objects_with_prefix("bucket", "prefix")
    assert result == ["a.json", "b.json"]


def test_list_objects_with_prefix_fail(mocker):
    """Should return empty list on exception."""
    mocker.patch.object(s3_utils.aws_connection, "S3Connector", side_effect=Exception("init fail"))
    result = s3_utils.list_objects_with_prefix("bucket", "prefix")
    assert result == []


# === select_latest_rerun ===
def test_select_latest_rerun_with_reruns():
    """Should return highest rerun file."""
    keys = ["file_rerun_1.json", "file_rerun_3.json", "file_rerun_2.json"]
    result = s3_utils.select_latest_rerun(keys, "file")
    assert result == "file_rerun_3.json"


def test_select_latest_rerun_no_rerun():
    """Should return base file when no rerun found."""
    keys = ["file.json"]
    result = s3_utils.select_latest_rerun(keys, "file")
    assert result == "file.json"


def test_select_latest_rerun_none():
    """Should return None when no matching file found."""
    keys = ["other.json"]
    result = s3_utils.select_latest_rerun(keys, "file")
    assert result is None

# === write_file_to_s3 ===
def test_write_file_to_s3_success(mocker, mock_client):
    """Should upload file buffer successfully."""
    # Mock put_object to simulate success
    mocker.patch.object(s3_utils, "put_object", return_value={"status": StatusEnum.SUCCESS})
    mocker.patch.object(
        s3_utils.aws_connection,
        "S3Connector",
        return_value=MagicMock(client=mock_client, bucket_name="test-bucket")
    )

    fake_buffer = io.BytesIO(b"some,data,to,upload")
    result = s3_utils.write_file_to_s3(fake_buffer, "test-bucket", "prefix/file.csv")

    assert result["status"] == StatusEnum.SUCCESS
    assert result["error"] is None
    assert result["s3_key_prefix"] == "prefix/file.csv"


def test_write_file_to_s3_fail_on_upload(mocker, mock_client):
    """Should return Failed when upload returns failed status."""
    mocker.patch.object(
        s3_utils, 
        "put_object", 
        return_value={"status": StatusEnum.FAILED, "error": "upload error"}
    )
    mocker.patch.object(
        s3_utils.aws_connection,
        "S3Connector",
        return_value=MagicMock(client=mock_client, bucket_name="test-bucket")
    )

    fake_buffer = io.BytesIO(b"invalid buffer")
    result = s3_utils.write_file_to_s3(fake_buffer, "test-bucket", "prefix/error.csv")

    assert result["status"] == StatusEnum.FAILED
    assert "upload error" in result["error"]
    assert result["s3_key_prefix"] == "prefix/error.csv"


def test_write_file_to_s3_exception(mocker, mock_client):
    """Should catch unexpected exception and return Failed."""
    mocker.patch.object(s3_utils, "put_object", side_effect=Exception("crash during upload"))
    mocker.patch.object(
        s3_utils.aws_connection,
        "S3Connector",
        return_value=MagicMock(client=mock_client, bucket_name="test-bucket")
    )

    fake_buffer = io.BytesIO(b"123")
    result = s3_utils.write_file_to_s3(fake_buffer, "test-bucket", "prefix/test.csv")

    assert result["status"] == StatusEnum.FAILED
    assert "crash during upload" in result["error"]
