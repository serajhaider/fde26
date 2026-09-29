import os
import re
import yaml
import logging
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Resolve the base directory (the fde26 directory containing this script)
# ---------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent

# Matches: ${VAR} or ${VAR:default}
ENV_PATTERN = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")

def _replace_env(value):
    """Replace ${VAR} or ${VAR:default} inside a string."""
    if not isinstance(value, str):
        return value
        
    def repl(match):
        var = match.group(1)
        default = match.group(2) or ""
        return os.getenv(var, default)

    return ENV_PATTERN.sub(repl, value)

def _walk(obj):
    """Recursively replace env vars in dict/list/string."""
    if isinstance(obj, dict):
        return {k: _walk(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk(v) for v in obj]
    if isinstance(obj, str):
        return _replace_env(obj)
    return obj

def load_config(config_file="config.yaml"):
    # 1. Explicitly load the .env file from the exact same directory as this script
    env_path = BASE_DIR / ".env"
    load_dotenv(dotenv_path=env_path, override=True)

    # 2. Simplified and robust search paths
    search_paths = [
        Path("/app") / config_file,  # Docker container fallback
        BASE_DIR / config_file,      # Local project path (fde26/config.yaml)
        Path.cwd() / config_file     # Current working directory fallback
    ]
    
    for path in search_paths:
        if path.exists():
            logger.info(f"Loaded configuration from: {path}")
            
            with open(path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                
            return _walk(config)

    raise FileNotFoundError(f"Could not find {config_file} in {search_paths}")

# Instantiate config on import
CONFIG = load_config()