import jwt
import grpc
import os, time,requests, threading, io
import pubsub_api_pb2 as pb2
import pubsub_api_pb2_grpc as pb2_grpc
import avro.schema
import avro.io
import certifi
import json
import anthropic

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
print("Authenticated with Salesforce, starting Pub/Sub subscription...‚")

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

print("Connecting to Salesforce Pub/Sub...\n")
with open(certifi.where(), 'rb') as f:
    creds = grpc.ssl_channel_credentials(f.read())
with grpc.secure_channel('api.pubsub.salesforce.com:7443', creds) as channel:
    stub = pb2_grpc.PubSubStub(channel)
    substream = stub.Subscribe(fetch_req_stream("/data/AccountChangeEvent"), metadata=auth_metadata)
    print("Subscribed to Salesforce Pub/Sub...\n")
    EventIndex = 0
    for response in substream:
        semaphore.release()
        latest_replay_id = response.latest_replay_id
        for event in response.events:
            schema = get_schema(stub, event.event.schema_id)
            decoded = decode(schema, event.event.payload)
            EventIndex +=1
            messages = Antrhopic_client.messages.create(
                model = "claude-sonnet-4-6",
                max_tokens = 1024,
                messages = [
                    {
                        "role": "user",
                       "content": f"""Convert this Salesforce event to a human readable format. {decoded}"""
                    }
                ]
            )
            print(f"{EventIndex} - Received event:", messages.content[0].text.strip())
         
