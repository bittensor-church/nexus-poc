# pyright: basic

import http.client
import urllib.parse
import uuid
from typing import Any
from unittest.mock import MagicMock

import pytest
from transform_test_utils import TransformActorTestSetupFactory
from utils import wait_until

from nexus.v1 import (
    ActorMisconfiguredException,
    NoopPayloadCreator,
    PresignedUrlCreator,
    S3ClientProvider,
    SafeInvokeWrappedException,
    WithPresignedUrl,
)


def _request_url(method: str, url: str, body: bytes | None = None) -> tuple[int, bytes]:
    parsed_url = urllib.parse.urlsplit(url)
    assert parsed_url.hostname is not None

    path_with_query = parsed_url.path
    if parsed_url.query:
        path_with_query = f"{path_with_query}?{parsed_url.query}"

    headers: dict[str, str] = {}
    if body is not None:
        headers["Content-Length"] = str(len(body))

    conn = http.client.HTTPConnection(parsed_url.hostname, parsed_url.port)
    try:
        conn.request(method, path_with_query, body=body, headers=headers)
        response = conn.getresponse()
        response_body = response.read()
        return response.status, response_body
    finally:
        conn.close()


def test_noop_payload_creator_passes_input_downstream_unchanged(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    creator = NoopPayloadCreator[str]("noop-payload-creator")
    setup = transform_actor_test_setup_factory(creator)

    with setup.running():
        ctx_id = setup.send(input_payload="input-payload")
        wait_until(lambda: len(setup.processed_collector.received_events) == 1)

    assert len(setup.error_collector.received_events) == 0
    event = setup.processed_collector.received_events[0]
    assert event.ctx_id == ctx_id
    assert event.payload == "input-payload"


def test_presigned_url_creator_put_generates_put_url_and_upload_succeeds(
    default_s3_storage_client,
    default_test_s3_bucket: str,
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    admin_client = default_s3_storage_client

    creator = PresignedUrlCreator[str](
        "presigner-put",
        bucket=default_test_s3_bucket,
        method="PUT",
    )
    setup = transform_actor_test_setup_factory(creator)

    with setup.running():
        ctx_id = setup.send(input_payload="input-payload")
        wait_until(lambda: len(setup.processed_collector.received_events) == 1)

    assert len(setup.error_collector.received_events) == 0
    received = setup.processed_collector.received_events[0]
    request_info: WithPresignedUrl[str] = received.payload
    assert request_info.input == "input-payload"

    presigned_url = request_info.presigned_url
    assert "X-Amz-Signature=" in presigned_url or "Signature=" in presigned_url
    assert f"/{default_test_s3_bucket}/" in presigned_url

    with setup.runtime.context_store.get_context(ctx_id) as context:
        s3_key = context.copy_user_data()[creator.id]
        assert uuid.UUID(s3_key).version == 7
        assert s3_key in presigned_url

    upload_data = b"payload-from-third-party"
    status, _ = _request_url("PUT", presigned_url, body=upload_data)
    assert status == 200

    uploaded = admin_client.get_object(Bucket=default_test_s3_bucket, Key=s3_key)
    assert uploaded["Body"].read() == upload_data


def test_presigned_url_creator_get_generates_get_url_and_download_succeeds(
    default_s3_storage_client,
    default_test_s3_bucket: str,
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    admin_client = default_s3_storage_client

    creator = PresignedUrlCreator[str](
        "presigner-get",
        bucket=default_test_s3_bucket,
        method="GET",
    )
    setup = transform_actor_test_setup_factory(creator)

    with setup.running():
        ctx_id = setup.send(input_payload="input-payload")
        wait_until(lambda: len(setup.processed_collector.received_events) == 1)

    assert len(setup.error_collector.received_events) == 0
    request_info: WithPresignedUrl[str] = setup.processed_collector.received_events[0].payload
    presigned_url = request_info.presigned_url

    with setup.runtime.context_store.get_context(ctx_id) as context:
        s3_key = context.copy_user_data()[creator.id]
        assert uuid.UUID(s3_key).version == 7
        assert s3_key in presigned_url

    download_data = b"payload-for-get"
    admin_client.put_object(Bucket=default_test_s3_bucket, Key=s3_key, Body=download_data)
    status, response_body = _request_url("GET", presigned_url)
    assert status == 200
    assert response_body == download_data


def test_presigned_url_creator_get_uses_existing_context_key_when_load_s3_key_set(
    default_s3_storage_client,
    default_test_s3_bucket: str,
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    admin_client = default_s3_storage_client
    context_key = "existing-s3-key"
    expected_s3_key = "known-get-key"

    creator = PresignedUrlCreator[str](
        "presigner-get-existing-key",
        bucket=default_test_s3_bucket,
        load_s3_key=context_key,
        method="GET",
    )
    setup = transform_actor_test_setup_factory(creator)

    with setup.runtime.context_store.create_context() as created_ctx:
        created_ctx.set_user_data(context_key, expected_s3_key)
        ctx_id = created_ctx.id

    with setup.running():
        setup.send(input_payload="input-payload", ctx_id=ctx_id)
        wait_until(lambda: len(setup.processed_collector.received_events) == 1)

    assert len(setup.error_collector.received_events) == 0
    presigned_url = setup.processed_collector.received_events[0].payload.presigned_url
    assert expected_s3_key in presigned_url

    with setup.runtime.context_store.get_context(ctx_id) as context:
        user_data = context.copy_user_data()
        assert user_data[context_key] == expected_s3_key
        assert creator.id not in user_data

    download_data = b"payload-existing-key-get"
    admin_client.put_object(Bucket=default_test_s3_bucket, Key=expected_s3_key, Body=download_data)
    status, response_body = _request_url("GET", presigned_url)
    assert status == 200
    assert response_body == download_data


def test_presigned_url_creator_put_uses_existing_context_key_when_load_s3_key_set(
    default_s3_storage_client,
    default_test_s3_bucket: str,
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    admin_client = default_s3_storage_client
    context_key = "existing-s3-key"
    expected_s3_key = "known-put-key"

    creator = PresignedUrlCreator[str](
        "presigner-put-existing-key",
        bucket=default_test_s3_bucket,
        load_s3_key=context_key,
        method="PUT",
    )
    setup = transform_actor_test_setup_factory(creator)

    with setup.runtime.context_store.create_context() as created_ctx:
        created_ctx.set_user_data(context_key, expected_s3_key)
        ctx_id = created_ctx.id

    with setup.running():
        setup.send(input_payload="input-payload", ctx_id=ctx_id)
        wait_until(lambda: len(setup.processed_collector.received_events) == 1)

    assert len(setup.error_collector.received_events) == 0
    presigned_url = setup.processed_collector.received_events[0].payload.presigned_url
    assert expected_s3_key in presigned_url

    with setup.runtime.context_store.get_context(ctx_id) as context:
        user_data = context.copy_user_data()
        assert user_data[context_key] == expected_s3_key
        assert creator.id not in user_data

    upload_data = b"payload-existing-key-put"
    status, _ = _request_url("PUT", presigned_url, body=upload_data)
    assert status == 200

    uploaded = admin_client.get_object(Bucket=default_test_s3_bucket, Key=expected_s3_key)
    assert uploaded["Body"].read() == upload_data


@pytest.mark.usefixtures("default_s3_storage_client")
def test_presigned_url_creator_does_not_create_new_key_when_load_s3_key_set(
    default_test_s3_bucket: str,
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    context_key = "existing-s3-key"
    expected_s3_key = "known-no-new-key"

    creator = PresignedUrlCreator[str](
        "presigner-no-new-key",
        bucket=default_test_s3_bucket,
        load_s3_key=context_key,
        method="GET",
    )
    setup = transform_actor_test_setup_factory(creator)

    with setup.runtime.context_store.create_context() as created_ctx:
        created_ctx.set_user_data(context_key, expected_s3_key)
        ctx_id = created_ctx.id

    with setup.running():
        setup.send(input_payload="input-payload", ctx_id=ctx_id)
        wait_until(lambda: len(setup.processed_collector.received_events) == 1)

    assert len(setup.error_collector.received_events) == 0
    presigned_url = setup.processed_collector.received_events[0].payload.presigned_url
    assert expected_s3_key in presigned_url

    with setup.runtime.context_store.get_context(ctx_id) as context:
        user_data = context.copy_user_data()
        assert user_data[context_key] == expected_s3_key
        assert creator.id not in user_data


@pytest.mark.parametrize("invalid_expiration", [0, -1])
def test_presigned_url_creator_rejects_non_positive_expiration(invalid_expiration: int) -> None:
    with pytest.raises(ActorMisconfiguredException, match="presigned_url_expiration_seconds must be > 0"):
        PresignedUrlCreator[str](
            "presigner-invalid-expiration",
            bucket="uploads",
            method="PUT",
            presigned_url_expiration_seconds=invalid_expiration,
        )


def test_presigned_url_creator_unsupported_method_emits_error(
    default_test_s3_bucket: str,
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    creator = PresignedUrlCreator[str](
        "presigner-unsupported-method",
        bucket=default_test_s3_bucket,
        method="POST",
    )
    setup = transform_actor_test_setup_factory(creator)

    with setup.running():
        _ = setup.send(input_payload="input-payload")
        wait_until(lambda: len(setup.error_collector.received_events) == 1)

    assert len(setup.processed_collector.received_events) == 0
    error_payload = setup.error_collector.received_events[0].payload
    assert isinstance(error_payload, ActorMisconfiguredException)
    assert "Unsupported HTTP method for presigned URL" in str(error_payload)


def test_presigned_url_creator_missing_load_s3_key_emits_error(
    default_test_s3_bucket: str,
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    creator = PresignedUrlCreator[str](
        "presigner-missing-context-key",
        bucket=default_test_s3_bucket,
        load_s3_key="missing-key",
        method="GET",
    )
    setup = transform_actor_test_setup_factory(creator)

    with setup.running():
        _ = setup.send(input_payload="input-payload")
        wait_until(lambda: len(setup.error_collector.received_events) == 1)

    assert len(setup.processed_collector.received_events) == 0
    error_payload = setup.error_collector.received_events[0].payload
    assert isinstance(error_payload, SafeInvokeWrappedException)
    assert isinstance(error_payload.__cause__, KeyError)


class _StaticS3ClientProvider(S3ClientProvider):
    def __init__(self, client: Any) -> None:
        self._client = client

    def get_client(self):  # type: ignore[override]
        return self._client


def test_presigned_url_creator_passes_expiration_to_generate_presigned_url(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    mock_s3_client = MagicMock()
    mock_s3_client.generate_presigned_url.return_value = "https://example.local/signed"

    creator = PresignedUrlCreator[str](
        "presigner-expiration-check",
        bucket="uploads",
        method="PUT",
        presigned_url_expiration_seconds=123,
        s3_client_provider=_StaticS3ClientProvider(mock_s3_client),
    )
    setup = transform_actor_test_setup_factory(creator)

    with setup.running():
        _ = setup.send(input_payload="input-payload")
        wait_until(lambda: len(setup.processed_collector.received_events) == 1)

    assert len(setup.error_collector.received_events) == 0
    mock_s3_client.generate_presigned_url.assert_called_once()
    kwargs = mock_s3_client.generate_presigned_url.call_args.kwargs
    assert kwargs["ExpiresIn"] == 123
    assert kwargs["ClientMethod"] == "put_object"
    assert kwargs["HttpMethod"] == "PUT"
