# pyright: basic

import logging
import os
from datetime import timedelta

from botocore.config import Config
from nexus._internal.actors.s3_client_provider import DefaultS3ClientProvider
from nexus.v1 import (
    AsyncHttpNeuronCommunicator,
    BlockCount,
    EveryTaskResultSampler,
    ImageUrlField,
    MultiOpenRouterPayloadCreator,
    NexusTask,
    NexusTaskName,
    NexusValidator,
    NoopPayloadCreator,
    NoopRouter,
    OpenRouterInferenceCommunicator,
    OpenRouterInferenceRequest,
    PresignedUrlCreator,
    RestEntryPoint,
    RetryStrategy,
    RoundRobinNeuronRouter,
    ScalarField,
    SetWeightsBeatNode,
    SuccessfulTaskResult,
    WeightSetterNode,
    miners_only,
)

from cat_images.subnet_models import (
    MinerPayload,
    MinerPublicResult,
    MinerResult,
    TaskScores,
    UserImageInput,
)

from . import weighing_algorithm
from .validator_settings import CatValidatorSettings

MINING_TASK_NAME = NexusTaskName("add-cat-to-image")
VALIDATION_TASK_NAME = NexusTaskName("validation-task")

# TEMP STUB (mTLS smoke test): DefaultS3ClientProvider hardcodes virtual-hosted-style addressing,
# which only resolves against real AWS S3 (bucket.host). MinIO (used here for local testing,
# detected via boto3's own AWS_ENDPOINT_URL) has no wildcard DNS for per-bucket subdomains and
# needs path-style (host/bucket) instead, or the miner's presigned PUT fails with a DNS error.
_addressing_style = "path" if os.environ.get("AWS_ENDPOINT_URL") else "virtual"
_MINER_UPLOAD_S3_CLIENT_PROVIDER = DefaultS3ClientProvider(config=Config(s3={"addressing_style": _addressing_style}))

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s", datefmt="%H:%M:%S", level=logging.INFO
)
log = logging.getLogger("validator")


class Validator(NexusValidator):
    """Validator graph for the cat-images subnet, including mining, validation, and weight setting."""

    def __init__(self, settings: CatValidatorSettings) -> None:
        super().__init__(settings)

        self.entry = RestEntryPoint(
            _id="cat-images-user-requests",
            path="/cat-images",
            port=settings.rest_entry_point_port,
            user_data_model=UserImageInput,
        )

        self.mining_task = NexusTask(
            name=MINING_TASK_NAME,
            retry=RetryStrategy("mining-task-retry", max_attempts=6, delay=timedelta(seconds=1.0)),
            payload_creator=PresignedUrlCreator(
                "miner-upload-url",
                bucket=settings.s3_bucket,
                method="PUT",
                s3_client_provider=_MINER_UPLOAD_S3_CLIENT_PROVIDER,
            ),
            router=RoundRobinNeuronRouter(
                "mining-router",
                netuid=settings.netuid,
                neuron_filter=miners_only,
            ),
            executor_communicator=AsyncHttpNeuronCommunicator(
                "miner-communicator",
                target_path="/process",
                callback_port=settings.miner_callback_port,
                callback_path="/mined-image",
                callback_base_url=f"http://{settings.external_ip}:{settings.miner_callback_port}",
                send_timeout=timedelta(seconds=1),
                total_processing_timeout=timedelta(seconds=60),
                input_model=MinerPayload,
                output_model=MinerResult,
            ),
            executor_result_converter=PresignedUrlCreator(
                "create-get-url-for-miner-image",
                method="GET",
                load_s3_key="miner-upload-url",
                bucket=settings.s3_bucket,
                s3_client_provider=_MINER_UPLOAD_S3_CLIENT_PROVIDER,
            ),
        )

        self.miner_result_sampler = EveryTaskResultSampler[MinerPayload, MinerResult, MinerPublicResult](
            "miner-result-sampler"
        )

        self.validation_task = NexusTask[
            tuple[SuccessfulTaskResult[MinerPayload, MinerResult, MinerPublicResult], ...],
            OpenRouterInferenceRequest,
            TaskScores,
        ](
            name=VALIDATION_TASK_NAME,
            retry=RetryStrategy("validation-task-retry", max_attempts=1, delay=timedelta(seconds=1.0)),
            payload_creator=MultiOpenRouterPayloadCreator[
                SuccessfulTaskResult[MinerPayload, MinerResult, MinerPublicResult]
            ](
                "create-payload-for-validation-task",
                user_prompt=settings.validation_prompt,
                item_selector=lambda task_result: {
                    "original_image_url": ImageUrlField(url=str(task_result.executor_payload.input.image_s3_url)),
                    "generated_image_url": ImageUrlField(url=str(task_result.executor_public_output.presigned_url)),
                    "task_result_id": ScalarField(value=str(task_result.id)),
                },
            ),
            router=NoopRouter[OpenRouterInferenceRequest]("validation-router"),
            executor_communicator=OpenRouterInferenceCommunicator[TaskScores](
                "validator-communicator",
                output_model=TaskScores,
            ),
            executor_result_converter=NoopPayloadCreator[TaskScores]("validation-result-converter"),
        )

        self.set_weights_beat = SetWeightsBeatNode(
            "weight-setting-trigger",
            netuid=settings.netuid,
            epoch_start_offset=BlockCount(20),
        )

        self.weight_setter = WeightSetterNode(
            "cat-images-weight-setter",
            weighing_func=lambda task_results_bundle: weighing_algorithm.weighing_func(
                MINING_TASK_NAME, VALIDATION_TASK_NAME, task_results_bundle
            ),
        )

        # mining
        self.connect(self.entry.source, self.mining_task.input)
        self.connect(self.mining_task.executor_output, self.entry.sink)
        self.connect(self.mining_task.error, self.entry.sink)

        # validation
        self.connect(self.mining_task.successful_task_result, self.miner_result_sampler.task_results)
        self.connect(self.miner_result_sampler.sampled_batch, self.validation_task.input)

        # weight setting
        self.connect(self.subnet_clock.source, taps=[self.set_weights_beat.block_beat])
        self.connect(self.set_weights_beat.source, self.weight_setter.sink)
