import jwt
import os
os.environ["GRPC_VERBOSITY"] = "NONE" #annoying cython warnings go awayyyyy
os.environ["GRPC_ENABLE_FORK_SUPPORT"] = "false"
import grpc
import os, time, requests, threading, io
import pubsub_api_pb2 as pb2
import pubsub_api_pb2_grpc as pb2_grpc
import avro.schema
import avro.io
import certifi
import json
import anthropic
import random

# ╔══════════════════════════════════════════╗
# ║   ANSI color codes — pure terminal rock  ║
# ╚══════════════════════════════════════════╝
class C:
    RESET   = '\033[0m'
    BOLD    = '\033[1m'
    DIM     = '\033[2m'
    RED     = '\033[91m'
    GREEN   = '\033[92m'
    YELLOW  = '\033[93m'
    CYAN    = '\033[96m'
    WHITE   = '\033[97m'
    PINK    = '\033[38;5;213m'
    ORANGE  = '\033[38;5;208m'
    PURPLE  = '\033[38;5;141m'


def banner():
    art = f"""
{C.PURPLE}{C.BOLD}     ⚡⚡⚡   S A L E S F O R C E   L I V E   ⚡⚡⚡{C.RESET}
{C.PINK}     ╔═══════════════════════════════════════════════╗
     ║      {C.YELLOW}🎸  COOLEST EVENT STREAM  🎸{C.PINK}         ║
     ║                       
     ╚═══════════════════════════════════════════════╝{C.RESET}
"""
    print(art)


def rock_print(msg, color=C.CYAN, emoji="🎸"):
    print(f"{color}{emoji}  {msg}{C.RESET}")


def loading_riff(msg, beats=6):
    """Tiny inline animation — strums while we wait."""
    frames = ["🎸 ", " 🎸", "🤘 ", " 🤘", "⚡ ", " ⚡"]
    for i in range(beats):
        print(f"\r{C.YELLOW}{frames[i % len(frames)]} {msg}{C.RESET}", end="", flush=True)
        time.sleep(0.12)
    print(f"\r{C.GREEN}✅ {msg} — done!{C.RESET}{' ' * 10}")


banner()

Antrhopic_client = anthropic.Anthropic()


semaphore = threading.Semaphore(1)
latest_replay_id = None

CLIENT_ID = os.environ.get("SF_ClIENT_ID")
USERNAME = os.environ.get("SF_USERNAME")
LOGIN_URL = "https://login.salesforce.com"
PRIVATE_KEY_FILE = os.environ.get("SF_JWT_SERVER_SECRET")


with open(PRIVATE_KEY_FILE, "r") as f:
    private_key = f.read()


claim = {
    "iss": CLIENT_ID,
    "sub": USERNAME,
    "aud": LOGIN_URL,
    "exp": int(time.time()) + 300
}


signed_jwt = jwt.encode(claim, private_key, algorithm="RS256")

loading_riff("Tuning the guitar — signing JWT")

response = requests.post(f"{LOGIN_URL}/services/oauth2/token", data={
    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
    "assertion": signed_jwt
})

token_data = response.json()


access_token = token_data["access_token"]
instance_url = token_data["instance_url"]
org_id = token_data["id"].split("/")[-2]

auth_metadata = (
    ("accesstoken", access_token),
    ("instanceurl", instance_url),
    ("tenantid", org_id),
)


schema_cache = {}
rock_print("Authenticated with Salesforce — amp's plugged in 🔌", C.GREEN, "✅")


def get_schema(stub, schema_id):
    if schema_id not in schema_cache:
        schema_json = stub.GetSchema(pb2.SchemaRequest(schema_id=schema_id), metadata=auth_metadata).schema_json
        schema_cache[schema_id] = avro.schema.parse(schema_json)
    return schema_cache[schema_id]


def decode(schema, payload):
    buf = io.BytesIO(payload)
    decoder = avro.io.BinaryDecoder(buf)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)


def fetch_req_stream(topic):
    while True:
        semaphore.acquire()
        yield pb2.FetchRequest(
            topic_name=topic,
            replay_preset=pb2.ReplayPreset.LATEST,
            num_requested=1,
        )


rock_print("Mic check 1, 2 — connecting to Salesforce Pub/Sub", C.CYAN, "📡")
with open(certifi.where(), 'rb') as f:
    creds = grpc.ssl_channel_credentials(f.read())
with grpc.secure_channel('api.pubsub.salesforce.com:7443', creds) as channel:
    stub = pb2_grpc.PubSubStub(channel)
    substream = stub.Subscribe(fetch_req_stream("/data/AccountChangeEvent"), metadata=auth_metadata)
    rock_print("STAGE IS LIVE — listening for events  🤘🤘🤘", C.PINK, "🔥")
    print(f"{C.DIM}{'─' * 64}{C.RESET}")
    EventIndex = 0
    SOLOS = ["🎸", "🥁", "🎤", "🎶", "🎺", "⚡", "🔥", "💥", "🤘", "🎷"]
    for response in substream:
        semaphore.release()
        latest_replay_id = response.latest_replay_id
        for event in response.events:
            schema = get_schema(stub, event.event.schema_id)
            decoded = decode(schema, event.event.payload)
            EventIndex += 1
            messages = Antrhopic_client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1024,
                messages=[
                    {
                        "role": "user",
                        "content": f"""You're the wildest rock & roll stadium announcer covering the Salesforce world tour. Convert this Salesforce change event into a SHORT (1-2 sentences MAX), witty, high-energy announcement.

Rules of the show:
- Be specific about WHAT changed: which fields, old value -> new value when present.
- Lean into rock metaphors, swagger, dramatic flair, punchy language.
- 1-2 emojis max — don't overdo it.
- No intro, no outro, no "Here's your announcement" — just the line itself.
- If it's a create, treat it like a band's debut. If it's an update, treat it like a riff change. If it's a delete, treat it like the lights cutting out mid-song.

Event data:
{decoded}"""
                    }
                ]
            )
            emoji = random.choice(SOLOS)
            print(f"{C.ORANGE}{C.BOLD}{emoji}  EVENT #{EventIndex}{C.RESET}  {C.DIM}│  replay_id captured{C.RESET}")
            print(f"{C.WHITE}   {messages.content[0].text.strip()}{C.RESET}")
            print(f"{C.DIM}{'─' * 64}{C.RESET}")