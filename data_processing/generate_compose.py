import yaml

GC_REPLICAS = 50
NORMAL_REPLICAS = 0  # whatever split you want

services = {}

for i in range(1, GC_REPLICAS + 1):
    name = f"gc-server-{i}"
    services[name] = {
        "image": "crdt-server:latest",
        "networks": ["crdt-net"],
        "environment": {
            "NODE_NAME": name,
            "LISTEN_HOST": name,
            "LISTEN_PORT": "9000",
            "CLIENT_PORT": "9100",
            "GC_REPLICA": "true",
            "METRICS_FILE_PATH": "/logs/metrics.jsonl",
            "S3_ENDPOINT": "${S3_ENDPOINT}",
            "S3_BUCKET": "${S3_BUCKET}",
            "S3_REGION": "${S3_REGION}",
            "S3_ACCESS_KEY": "${AWS_ACCESS_KEY_ID}",
            "S3_SECRET_KEY": "${AWS_SECRET_ACCESS_KEY}",
        },
        "volumes": [f"./logs/{name}/:/logs/"],
        "restart": "unless-stopped",
    }

for i in range(1, NORMAL_REPLICAS + 1):
    name = f"normal-server-{i}"
    services[name] = {
        "image": "crdt-server:latest",
        "networks": ["crdt-net"],
        "environment": {
            "NODE_NAME": name,
            "LISTEN_HOST": name,
            "LISTEN_PORT": "9000",
            "CLIENT_PORT": "9100",
            "METRICS_FILE_PATH": "/logs/metrics.jsonl",
            "S3_ENDPOINT": "${S3_ENDPOINT}",
            "S3_BUCKET": "${S3_BUCKET}",
            "S3_REGION": "${S3_REGION}",
            "S3_ACCESS_KEY": "${AWS_ACCESS_KEY_ID}",
            "S3_SECRET_KEY": "${AWS_SECRET_ACCESS_KEY}",
        },
        "volumes": [f"./logs/{name}/:/logs/"],
        "restart": "unless-stopped",
    }

compose = {
    "networks": {"crdt-net": {"driver": "bridge"}},
    "services": services,
}

with open("../docker-compose.generated.yml", "w") as f:
    yaml.dump(compose, f, default_flow_style=False)
