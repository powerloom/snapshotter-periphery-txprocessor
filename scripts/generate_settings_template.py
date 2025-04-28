import json
import os

# Define the template structure matching utils/models/settings_model.py
def generate_template():
    """Generate template settings.json with placeholder values"""
    template = {
        "namespace": "${NAMESPACE}",
        "rpc": {
            "url": "${RPC_URL}",
            "retry": "${RPC_RETRY}",
            "request_time_out": "${RPC_TIMEOUT}"
        },
        "redis": {
            "host": "${REDIS_HOST}",
            "port": "${REDIS_PORT}",
            "db": "${REDIS_DB}",
            "password": "${REDIS_PASSWORD}",
            "ssl": "${REDIS_SSL}",
            "cluster_mode": "${REDIS_CLUSTER}",
            "data_retention": {
                "max_blocks": "${REDIS_MAX_BLOCKS}",
                "ttl_seconds": "${REDIS_TTL_SECONDS}"
            }
        },
        "logs": {
            "debug_mode": "${LOG_DEBUG}",
            "write_to_files": "${LOG_TO_FILES}",
            "level": "${LOG_LEVEL}"
        },
        "processor": {
            "redis_queue_key": "${PROCESSOR_QUEUE_KEY}",
            "redis_block_timeout": "${PROCESSOR_BLOCK_TIMEOUT}"
        }
    }

    # Ensure config directory exists
    config_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config') # Assumes scripts/ is one level below root
    os.makedirs(config_dir, exist_ok=True)
    template_path = os.path.join(config_dir, 'settings.template.json')

    with open(template_path, 'w') as f:
        json.dump(template, f, indent=2)
    print(f"Generated template at {template_path}")

if __name__ == "__main__":
    generate_template()