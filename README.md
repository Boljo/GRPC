<h1 align="center">🎸 Salesforce Live</h1>

<p align="center">
  Stream Salesforce Change Data Capture events in real time and have Claude announce each one like a rock & roll stadium MC.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.9%2B-blue" alt="Python 3.9+">
  <img src="https://img.shields.io/badge/Claude-sonnet--4--6-8A2BE2" alt="Claude Sonnet 4.6">
  <img src="https://img.shields.io/badge/Salesforce-Pub%2FSub%20API-00A1E0" alt="Salesforce Pub/Sub API">
</p>

## 📺 Demo

<p align="center">
  <a href="https://www.youtube.com/watch?v=_vNpW9AhxWA">
    <img src="https://img.youtube.com/vi/_vNpW9AhxWA/maxresdefault.jpg" alt="Watch the demo" width="600">
  </a>
</p>

## How it works

1. Authenticates to Salesforce via the JWT Bearer OAuth flow.
2. Opens a gRPC stream to the Salesforce Pub/Sub API and subscribes to `/data/AccountChangeEvent`.
3. Decodes each Avro-encoded payload (schemas cached per `schema_id`).
4. Sends the decoded event to Claude (`claude-sonnet-4-6`) with a stadium-announcer prompt.
5. Prints the result to the terminal.

## Setup

```bash
pip install pyjwt cryptography grpcio grpcio-tools requests avro certifi anthropic
```

Generate the Salesforce protobuf stubs:

```bash
curl -O https://raw.githubusercontent.com/developerforce/pub-sub-api/main/pubsub_api.proto
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. pubsub_api.proto
```

Set environment variables:

| Variable | Description |
|---|---|
| `SF_ClIENT_ID` | Consumer Key of your Salesforce Connected App (note the lowercase `l`) |
| `SF_USERNAME` | Pre-authorized user for the JWT flow |
| `SF_JWT_SERVER_SECRET` | Path to the RSA private key (PEM) matching the cert on the Connected App |
| `ANTHROPIC_API_KEY` | Anthropic API key |

Prereqs in Salesforce: a Connected App configured for JWT Bearer, and Change Data Capture enabled for `Account`.

## Run

```bash
python salesforce_live.py
```

Then create, update, or delete an Account record. The announcement appears within a couple of seconds.

## Configuration

- **Topic** — hardcoded to `/data/AccountChangeEvent`. Change to any CDC channel or Platform Event topic.
- **Replay preset** — `LATEST` (only new events). Switch to `EARLIEST` in `fetch_req_stream` for backlog.
- **Throughput** — one event at a time via a `Semaphore(1)`. Bump both the semaphore count and `num_requested` to parallelize.

## Common issues

- `invalid_grant: user hasn't approved this consumer` → pre-authorize the user on the Connected App.
- `invalid_grant: invalid assertion` → private key doesn't match the uploaded cert, or wrong `aud` (use `https://test.salesforce.com` for sandboxes).
- No events arriving → confirm CDC is enabled for `Account` in Setup.
