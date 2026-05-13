from pathlib import Path

import yaml

from config import DEFAULT_HOST_YAML, GENERATOR_KEYS, SERVER_KEYS


def load_host_yaml(host_yaml_path: Path) -> dict:
    if host_yaml_path.exists():
        return yaml.safe_load(host_yaml_path.read_text(encoding="utf-8"))
    return DEFAULT_HOST_YAML.copy()


def save_host_yaml(config: dict, host_yaml_path: Path) -> None:
    host_yaml_path.write_text(
        yaml.dump(config, default_flow_style=False, allow_unicode=True),
        encoding="utf-8",
    )


def apply_host_yaml_options(opts: dict, host_yaml_path: Path) -> dict:
    config = load_host_yaml(host_yaml_path)
    originals = {}
    for key, value in opts.items():
        if key in SERVER_KEYS:
            originals[("server_options", key)] = config["server_options"].get(key)
            config["server_options"][key] = value
        elif key in GENERATOR_KEYS:
            originals[("generator", key)] = config["generator"].get(key)
            config["generator"][key] = value
    save_host_yaml(config, host_yaml_path)
    return originals


def restore_host_yaml(originals: dict, host_yaml_path: Path) -> None:
    config = load_host_yaml(host_yaml_path)
    for (section, key), value in originals.items():
        config[section][key] = value
    save_host_yaml(config, host_yaml_path)
