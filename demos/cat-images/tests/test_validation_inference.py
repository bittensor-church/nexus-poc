# pyright: basic

import queue
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from nexus.v1 import (
    BlockBeat,
    BlockHash,
    BlockNumber,
    ContextStore,
    EveryTaskResultSampler,
    ImageUrlField,
    InMemoryContextStorePersistence,
    MultiOpenRouterPayloadCreator,
    MultiOpenRouterPayloadCreatorActor,
    NetUid,
    OpenRouterInferenceRequest,
    S3PresignedUrl,
    ScalarField,
    SuccessfulTaskResult,
    TaskResultId,
    Timestamp,
)

from cat_images.subnet_models import (
    ImageHash,
    MinerPayload,
    MinerPublicResult,
    MinerResult,
    S3Url,
    UserImageInput,
)
from cat_images.validator import CatValidatorSettings, Validator


@dataclass(frozen=True)
class _TargetStub:
    """Minimal target stub used to construct successful task results in tests."""

    hotkey: str


def _block_beat(block_number: int) -> BlockBeat:
    return BlockBeat(
        block_number=BlockNumber(block_number),
        block_timestamp=Timestamp(block_number * 1000),
        block_hash=BlockHash(f"0x{block_number:064x}"),
    )


def _successful_task_result(
    *,
    raw_id: int,
    original_image_url: str = "https://source.test/original.png",
    generated_image_url: str = "https://result.test/cat.png",
) -> SuccessfulTaskResult[MinerPayload, MinerResult, MinerPublicResult]:
    processing_started = datetime(2026, 4, 9, tzinfo=UTC)
    return SuccessfulTaskResult(
        id=TaskResultId(uuid.UUID(int=raw_id)),
        processing_started=processing_started,
        processing_finished=processing_started + timedelta(seconds=1),
        block_at_finish=_block_beat(123),
        executor_payload=MinerPayload(
            input=UserImageInput(image_s3_url=S3Url(original_image_url)),
            presigned_url=S3PresignedUrl("https://upload.test/request"),
        ),
        target=cast(Any, _TargetStub(hotkey="miner-hotkey")),
        executor_output=MinerResult(image_hash=ImageHash("hash-1")),
        executor_public_output=MinerPublicResult(
            input=MinerResult(image_hash=ImageHash("hash-1")),
            presigned_url=S3PresignedUrl(generated_image_url),
        ),
    )


def _render_request(
    task_result: SuccessfulTaskResult[MinerPayload, MinerResult, MinerPublicResult],
) -> OpenRouterInferenceRequest:
    validator = Validator(_validator_settings())
    creator = cast(
        MultiOpenRouterPayloadCreator[SuccessfulTaskResult[MinerPayload, MinerResult, MinerPublicResult]],
        validator.validation_task.payload_creator,
    )
    context_store = ContextStore.recover_from(InMemoryContextStorePersistence()).context_store
    actor = cast(
        MultiOpenRouterPayloadCreatorActor[SuccessfulTaskResult[MinerPayload, MinerResult, MinerPublicResult]],
        creator.build_actor(pipe_to_bus=queue.Queue(), context_store=context_store),
    )

    with context_store.create_context() as ctx:
        return actor._transform(ctx, (task_result,))


def _validator_settings() -> CatValidatorSettings:
    return CatValidatorSettings(
        netuid=NetUid(1),
        openrouter_api_key="validation-api-key",
        external_ip="127.0.0.1",
        pylon_service_address="https://pylon.test",
        pylon_open_access_token="token",
    )


def test_validator_validation_task_item_selector_extracts_prompt_fields_from_successful_task_result() -> None:
    validator = Validator(_validator_settings())
    creator = cast(
        MultiOpenRouterPayloadCreator[SuccessfulTaskResult[MinerPayload, MinerResult, MinerPublicResult]],
        validator.validation_task.payload_creator,
    )
    task_result = _successful_task_result(raw_id=1)

    selected = creator.item_selector(task_result)
    assert selected is not None

    assert list(selected) == ["original_image_url", "generated_image_url", "task_result_id"]
    assert selected["original_image_url"] == ImageUrlField(url="https://source.test/original.png")
    assert selected["generated_image_url"] == ImageUrlField(url="https://result.test/cat.png")
    assert selected["task_result_id"] == ScalarField(value=str(task_result.id))


def test_validator_validation_task_item_selector_renders_prompt_content_in_image_first_order() -> None:
    task_result = _successful_task_result(raw_id=1)
    settings = _validator_settings()

    request = _render_request(task_result)

    content = request.messages[0]["content"]

    assert content == [
        {"type": "text", "text": settings.validation_prompt},
        {"type": "text", "text": "item[0].original_image_url:"},
        {"type": "image_url", "image_url": {"url": "https://source.test/original.png"}},
        {"type": "text", "text": "item[0].generated_image_url:"},
        {"type": "image_url", "image_url": {"url": "https://result.test/cat.png"}},
        {"type": "text", "text": f"item[0].task_result_id: {task_result.id}"},
    ]


def test_validator_connects_successful_mining_task_results_into_sampler() -> None:
    validator = Validator(_validator_settings())

    assert isinstance(validator.miner_result_sampler, EveryTaskResultSampler)
    assert validator.subnet_flow.pipes[validator.mining_task.successful_task_result].primary == (
        validator.miner_result_sampler.task_results
    )
    assert validator.subnet_flow.pipes[validator.miner_result_sampler.sampled_batch].primary == (
        validator.validation_task.input
    )


def test_validator_connects_weight_setting_as_a_subnet_clock_tap() -> None:
    validator = Validator(_validator_settings())

    targets = validator.subnet_flow.pipes[validator.subnet_clock.source]

    assert targets.primary is None
    assert targets.taps == frozenset({validator.set_weights_beat.block_beat})
