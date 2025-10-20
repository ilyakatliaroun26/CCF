# src/utils.py
import yaml
from pathlib import Path


def load_config(config_path: str = "config.yaml") -> dict:
    """
    Loads a YAML configuration file as a dictionary.
    Works reliably regardless of where the script or notebook is executed.
    """
    # Resolve the project root (one level above src/)
    project_root = Path(__file__).resolve().parents[1]

    # Build the absolute path to the config file
    config_file = (project_root / config_path).resolve()

    # Validate existence
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_file}")

    # Load YAML safely
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    return config
