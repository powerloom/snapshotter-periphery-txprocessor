#!/usr/bin/env python3
import os
import json
import subprocess
from string import Template
from dotenv import load_dotenv

CONFIG_DIR = 'config' # Relative to WORKDIR (/app)
TEMPLATE_FILE = os.path.join(CONFIG_DIR, 'settings.template.json')
SETTINGS_FILE = os.path.join(CONFIG_DIR, 'settings.json')

def fill_template():
    """Fill settings template with environment variables"""
    load_dotenv() # Load .env file if present

    if not os.path.exists(TEMPLATE_FILE):
        print(f"ERROR: Template file not found at {TEMPLATE_FILE}")
        exit(1)

    with open(TEMPLATE_FILE, 'r') as f:
        template = Template(f.read())

    # Prepare dictionary with defaults for substitution
    # Ensures all expected keys exist, even if ENV VAR is missing
    env_with_defaults = {
        "RPC_URL": os.getenv("RPC_URL", "http://localhost:8545"),
        "RPC_RETRY": os.getenv("RPC_RETRY", "3"),
        "RPC_TIMEOUT": os.getenv("RPC_TIMEOUT", "15"),
        "REDIS_HOST": os.getenv("REDIS_HOST", "localhost"),
        "REDIS_PORT": os.getenv("REDIS_PORT", "6379"),
        "REDIS_DB": os.getenv("REDIS_DB", "0"),
        "REDIS_PASSWORD": os.getenv("REDIS_PASSWORD", ""), # Empty string if not set
        "REDIS_SSL": os.getenv("REDIS_SSL", "false"),
        "REDIS_CLUSTER": os.getenv("REDIS_CLUSTER", "false"),
        "LOG_DEBUG": os.getenv("LOG_DEBUG", "false"),
        "LOG_TO_FILES": os.getenv("LOG_TO_FILES", "true"),
        "LOG_LEVEL": os.getenv("LOG_LEVEL", "INFO"),
        "PROCESSOR_QUEUE_KEY": os.getenv("PROCESSOR_QUEUE_KEY", "pending_transactions"),
        "PROCESSOR_BLOCK_TIMEOUT": os.getenv("PROCESSOR_BLOCK_TIMEOUT", "0")
        # Add any other env vars needed by the template
    }

    print("--- Substituting settings template ---")
    print(f"Using environment variables and defaults: {list(env_with_defaults.keys())}")

    try:
        filled_str = template.substitute(env_with_defaults)
        # Validate if it's valid JSON before writing
        json.loads(filled_str)
    except KeyError as e:
         print(f"ERROR: Missing environment variable for substitution: {e}. Check template and env vars.")
         exit(1)
    except json.JSONDecodeError as e:
         print(f"ERROR: Substituted template resulted in invalid JSON: {e}")
         print("--- Substituted Content ---")
         print(filled_str)
         print("--------------------------")
         exit(1)
    except Exception as e:
         print(f"ERROR: Failed during template substitution: {e}")
         exit(1)

    print(f"Writing final settings to {SETTINGS_FILE}")
    with open(SETTINGS_FILE, 'w') as f:
        f.write(filled_str)
    print("--- Settings substitution complete ---")


if __name__ == "__main__":
    fill_template()
    # Execute main application using python directly
    print("Executing main application: python main.py")
    # Using execvp to replace the current process is often better in containers
    # os.execvp("python", ["python", "main.py"])
    # Using subprocess for simplicity here:
    result = subprocess.run(["python", "main.py"], check=False)
    exit(result.returncode)
