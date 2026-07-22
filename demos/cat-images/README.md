# cat-images subnet

A subnet where miners add cats to images using AI image generation via OpenRouter.

## Development setup

```bash
uv sync --all-groups --all-extras
```

## Localnet compose

Use [RUNBOOK.md](RUNBOOK.md) to start pylon, validator, miner, and facilitator together against a host-run localnet.
The Docker builds use the repository root as the `bittensor-nexus-library` source so the demo package's `../..`
editable dependency resolves the same way in containers as it does locally.

## Facilitator (Catificator)

Web UI that lets users upload images, dispatches catification jobs to validators, and streams progress back via SSE.

### Configuration

```bash
cp .env.facilitator.example .env
# edit .env and fill in your S3 credentials and validator endpoints
```

| Variable | Default | Description |
|---|---|---|
| `FACI_S3_ENDPOINT_URL` | *(none — uses AWS)* | S3-compatible endpoint (set for MinIO, etc.) |
| `FACI_S3_BUCKET` | *(required)* | Bucket for uploaded images |
| `FACI_S3_ACCESS_KEY` | *(required)* | S3 access key |
| `FACI_S3_SECRET_KEY` | *(required)* | S3 secret key |
| `FACI_S3_REGION` | `""` | S3 region |
| `FACI_VALIDATORS` | *(required)* | JSON map of known validators: `{"hotkey": "http://host:port/cat-images"}` |
| `FACI_PORT` | `8080` | Port the facilitator listens on |
| `FACI_HOST` | `0.0.0.0` | Bind address |
| `FACI_SUBMIT_MAX_RETRIES` | `3` | Max retries when submitting a job to a validator |
| `FACI_SUBMIT_TIMEOUT_SECONDS` | `30.0` | Timeout per submission attempt |

### Running

```bash
uv run --group facilitator -m cat_images.facilitator
```

Open `http://localhost:8080` — upload an image, pick a validator, hit "Catify this!".

### Pages

- `/` — Upload form with live SSE progress
- `/jobs` — Job list
- `/validators` — Validator admin (toggle availability, delete)

### Status callback

Validators push status updates to `POST /api/jobs/{job_id}/status`. The payload is a discriminated union on `liveness`:

```json
{"liveness": "in_progress", "status": "Processing...", "timestamp": "2026-03-04T12:00:00Z"}
{"liveness": "success", "status": "Catified!", "timestamp": "...", "result": {"result_image_url": "https://...", "image_hash": "<sha256-like-hash>"}}
{"liveness": "failed", "status": "Out of cats", "timestamp": "..."}
```

## Validator

### Configuration

| Variable | Default | Description |
|---|---|---|
| `VALIDATOR_NETUID` | *(required)* | Subnet UID to validate and set weights on |
| `VALIDATOR_EXTERNAL_IP` | *(required)* | Address miners use to reach validator callbacks |
| `VALIDATOR_REST_ENTRY_POINT_PORT` | `8081` | Port for user/facilitator requests |
| `VALIDATOR_MINER_CALLBACK_PORT` | `9091` | Port for miner result callbacks |
| `VALIDATOR_PYLON_SERVICE_ADDRESS` | *(required)* | Pylon service URL |
| `VALIDATOR_PYLON_OPEN_ACCESS_TOKEN` | *(required)* | Pylon open-access token |
| `VALIDATOR_PYLON_IDENTITY_NAME` | *(optional)* | Pylon identity name for weight writes |
| `VALIDATOR_PYLON_IDENTITY_TOKEN` | *(optional)* | Pylon identity token for weight writes |
| `VALIDATOR_OPENROUTER_API_KEY` | *(required)* | OpenRouter API key used by validation inference |
| `VALIDATOR_OPENROUTER_URL` | `https://openrouter.ai/api/v1/chat/completions` | OpenRouter API endpoint |
| `VALIDATOR_OPENROUTER_MODEL` | `google/gemini-2.5-flash-image` | OpenRouter model for validation scoring |
| `VALIDATOR_OPENROUTER_TIMEOUT_SECONDS` | `120.0` | OpenRouter request timeout |
| `VALIDATOR_OPENROUTER_TEMPERATURE` | `0.0` | OpenRouter sampling temperature |
| `VALIDATOR_VALIDATION_PROMPT` | *(built in)* | Prompt used to score generated images |
| `VALIDATOR_S3_BUCKET` | `my-cat-images-bucket` | Bucket used for generated-image presigned URLs |

```bash
uv run -m cat_images.validator
```

Starts the validator on port 8081 at `/cat-images`. Accepts POST requests with:

```json
{"image_s3_url": "https://..."}
```

Returns JSON:

```json
{
  "input": {
    "image_hash": "<sha256-like-hash>"
  },
  "presigned_url": "https://<bucket>.s3.amazonaws.com/<key>?<query>"
}
```

For facilitator UI testing with `test_scripts/fake_validator.py`, the fake endpoint still listens on `/submit` and
returns `{"result_image_url":"...", "image_hash":"fake-hash"}`.

### Current graph

- `RestEntryPoint("/cat-images")` accepts `UserImageInput` and feeds `NexusTask("add-cat-to-image")`.
- The mining task creates a PUT presigned URL, routes to miners with `RoundRobinNeuronRouter(miners_only)`, sends work to
  miner `/process` endpoints with `AsyncHttpNeuronCommunicator`, and converts the miner upload key into a GET presigned
  URL.
- Successful mining results flow through `EveryTaskResultSampler` into `NexusTask("validation-task")`.
- The validation task builds a multimodal OpenRouter request with `MultiOpenRouterPayloadCreator`, runs it locally
  through `OpenRouterInferenceCommunicator`, and stores structured `TaskScores`.
- The validator's built-in `subnet_clock` (a `BlockBeatNode`) taps each Nexus Task and
  `SetWeightsBeatNode(epoch_start_offset=BlockCount(20))`, giving every clock consumer an independent child context.
  The weight-setting trigger gates attempts per epoch using `pylon.unstable.identity.get_weights_status` plus an
  in-memory cooldown. The "weights already set this epoch" flag is populated solely from pylon's response, so the
  epoch is silenced on the next block beat after pylon reports `weights_submitted=True`. When all gates pass, the node
  emits `SetWeightsBeat` to `WeightSetterNode`, which calculates and writes miner weights through pylon.

## Miner

### Configuration

```bash
cp .env.miner.example .env
# edit .env and fill in your OpenRouter API key
```

| Variable | Default | Description |
|---|---|---|
| `MINER_OPENROUTER_URL` | `https://openrouter.ai/api/v1/chat/completions` | OpenRouter API endpoint |
| `MINER_OPENROUTER_MODEL` | `google/gemini-2.5-flash-image` | Model to use for image generation |
| `MINER_PROMPT` | *(built-in cat prompt)* | Prompt sent to the model |
| `MINER_PORT` | `9090` | Port the miner listens on |
| `MINER_PATH` | `/process` | HTTP path for the miner endpoint |

#### Axon updater

When enabled, the miner periodically registers its serving address (IP/port) on the Bittensor chain and updates it if it drifts. Bittensor SDK work runs in an isolated subprocess to avoid polluting the main process.

| Variable | Default | Description |
|---|---|---|
| `MINER_UPDATE_AXON` | `false` | Enable automatic axon registration |
| `MINER_NETUID` | *(required when enabled)* | Subnet UID to register on |
| `MINER_EXTERNAL_IP` | *(auto-detected)* | IP address to advertise. `127.0.0.1`/`localhost` are rejected by subtensor — use `127.0.0.2` for local testing |
| `MINER_EXTERNAL_PORT` | same as `MINER_PORT` | Port to advertise (if different from listen port) |
| `MINER_SERVE_INTERVAL` | `60` (seconds) | How often to check and update axon info |
| `MINER_WALLET_NAME` | `default` | Bittensor wallet name |
| `MINER_HOTKEY_NAME` | `default` | Bittensor hotkey name |
| `MINER_SUBTENSOR_NETWORK` | `finney` | Subtensor network (`finney`, `test`, or a URL) |

### Running locally

```bash
uv run --group miner -m cat_images.miner
```

Starts the miner on port 9090 (default). Receives tasks from the validator, downloads the source image, generates a cat version via OpenRouter, uploads the result to S3, and sends back an image hash.

### Docker

Build (requires BuildKit, default since Docker 23+):

```bash
./build_miner.sh cat-miner:latest
```

Run:

```bash
docker run --rm -p 9090:9090 \
  -e MINER_OPENROUTER_API_KEY=sk-or-... \
  cat-miner
```

Or with an env file:

```bash
docker run --rm -p 9090:9090 --env-file .env cat-miner
```

When axon updates are enabled, the container needs access to the wallet keys:

```bash
docker run --rm -p 9090:9090 --env-file .env \
  -v "$HOME/.bittensor:/root/.bittensor:ro" \
  cat-miner
```

### Test script

`test_scripts/miner_test.py` sends an image to a running miner and saves the result locally. Spins up a local HTTP server that fakes S3 (serves the source image, accepts the upload) and receives the miner's callback.

```bash
uv run test_scripts/miner_test.py photo.png result.png
uv run test_scripts/miner_test.py photo.png result.png --miner-url http://10.0.0.5:9090/process
```

Requires the miner to be running separately.
