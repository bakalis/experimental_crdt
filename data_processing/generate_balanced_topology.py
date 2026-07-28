import yaml

GC_REPLICAS = 2
NORMAL_REPLICAS = 8


def assign_gc_to_normal(normal_count, gc_count):
    """Each normal node gets exactly one GC node, round-robin."""
    return {
        f"normal-server-{i}": f"gc-server-{(i - 1) % gc_count + 1}"
        for i in range(1, normal_count + 1)
    }


def assign_normals_to_gc(normal_to_gc):
    """Invert the mapping: each GC node gets its list of normal nodes."""
    gc_to_normals = {}
    for normal, gc in normal_to_gc.items():
        gc_to_normals.setdefault(gc, []).append(normal)
    return gc_to_normals


def make_server(name, is_gc, peers):
    env = {
        "NODE_NAME": name,
        "LISTEN_HOST": name,
        "LISTEN_PORT": "9000",
        "CLIENT_PORT": "9100",
        "METRICS_FILE_PATH": "/logs/metrics.jsonl",
        "DISCOVERY_CONNECT_NODE_IDS": ",".join(peers),
        "S3_ENDPOINT": "${S3_ENDPOINT}",
        "S3_BUCKET": "${S3_BUCKET}",
        "S3_REGION": "${S3_REGION}",
        "S3_ACCESS_KEY": "${AWS_ACCESS_KEY_ID}",
        "S3_SECRET_KEY": "${AWS_SECRET_ACCESS_KEY}",
    }
    if is_gc:
        env["GC_REPLICA"] = "true"

    return {
        "image": "crdt-server:latest",
        "networks": ["crdt-net"],
        "environment": env,
        "volumes": [f"./logs/{name}/:/logs/"],
        "restart": "unless-stopped",
    }


normal_to_gc = assign_gc_to_normal(NORMAL_REPLICAS, GC_REPLICAS)
gc_to_normals = assign_normals_to_gc(normal_to_gc)

services = {}

for i in range(1, GC_REPLICAS + 1):
    name = f"gc-server-{i}"
    normal_peers = gc_to_normals.get(name, [])
    gc_peers = [f"gc-server-{j}" for j in range(1, GC_REPLICAS + 1) if j != i]
    services[name] = make_server(name, is_gc=True, peers=normal_peers + gc_peers)

for i in range(1, NORMAL_REPLICAS + 1):
    name = f"normal-server-{i}"
    gc_peer = normal_to_gc[name]
    services[name] = make_server(name, is_gc=False, peers=[gc_peer])

compose = {
    "version": "3.9",
    "services": services,
    "networks": {"crdt-net": {"driver": "bridge"}},
}

with open("../docker-compose.generated.yml", "w") as f:
    yaml.dump(compose, f, default_flow_style=False)

# Print topology summary
print(f"Topology: {NORMAL_REPLICAS} normal, {GC_REPLICAS} GC")
print(f"Normal nodes per GC: {NORMAL_REPLICAS / GC_REPLICAS:.1f} avg")
for gc, normals in sorted(gc_to_normals.items()):
    print(f"  {gc}: {len(normals)} normal nodes")
