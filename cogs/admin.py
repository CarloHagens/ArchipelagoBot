import logging

import discord
import yaml
from discord import app_commands
from discord.ext import commands

from utils.autocomplete import version_autocomplete
from utils.versions import get_installed_versions, get_version_dir

log = logging.getLogger('bot')


def _find_paths(obj, target_key: str, path: tuple = ()) -> list[tuple]:
    """Recursively find all paths to a given key in a nested dict."""
    results = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            current = path + (k,)
            if str(k) == target_key:
                results.append(current)
            results.extend(_find_paths(v, target_key, current))
    return results


def _get_nested(obj, path: tuple):
    for key in path:
        obj = obj[key]
    return obj


def _set_nested(obj, path: tuple, value) -> None:
    for key in path[:-1]:
        obj = obj[key]
    obj[path[-1]] = value


class AdminCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="hostyaml", description="View or modify a key in host.yaml")
    @app_commands.describe(
        key="Key name, or dotted path for nested keys (e.g. stardew_valley_options.allow_jojapocalypse)",
        value="Value to set. Leave blank to view the current value.",
        version="Archipelago version to modify (default: latest)",
    )
    @app_commands.autocomplete(version=version_autocomplete)
    async def hostyaml(
        self,
        interaction: discord.Interaction,
        key: str,
        value: str = None,
        version: str = None,
    ):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("⚠️ This command requires administrator permissions.", ephemeral=True)
            return

        versions = get_installed_versions()
        if not versions:
            await interaction.response.send_message("⚠️ No Archipelago versions installed.", ephemeral=True)
            return

        version = version or versions[0]
        if version not in versions:
            await interaction.response.send_message(f"⚠️ Version `{version}` is not installed.", ephemeral=True)
            return

        host_yaml_path = get_version_dir(version) / "host.yaml"
        if not host_yaml_path.exists():
            await interaction.response.send_message(f"⚠️ `host.yaml` not found for version `{version}`.", ephemeral=True)
            return

        try:
            data = yaml.safe_load(host_yaml_path.read_text(encoding="utf-8"))
        except Exception as e:
            await interaction.response.send_message(f"⚠️ Failed to parse `host.yaml`: `{e}`", ephemeral=True)
            return

        # Resolve key — dotted path or simple key name
        parts = key.split(".")
        if len(parts) > 1:
            # Explicit dotted path
            path = tuple(parts)
            try:
                _get_nested(data, path)
            except (KeyError, TypeError):
                await interaction.response.send_message(
                    f"⚠️ Path `{key}` not found in `host.yaml`.", ephemeral=True
                )
                return
            paths = [path]
        else:
            # Search recursively for the key
            paths = _find_paths(data, key)
            if not paths:
                await interaction.response.send_message(
                    f"⚠️ Key `{key}` not found in `host.yaml`.", ephemeral=True
                )
                return
            if len(paths) > 1 and value is not None:
                path_strs = "\n".join("`.`".join(str(k) for k in p) for p in paths)
                await interaction.response.send_message(
                    f"⚠️ Key `{key}` appears in multiple places — use a dotted path to specify which:\n{path_strs}",
                    ephemeral=True,
                )
                return

        # View mode
        if value is None:
            lines = []
            for p in paths:
                current = _get_nested(data, p)
                path_str = ".".join(str(k) for k in p)
                lines.append(f"`{path_str}` = `{current}`")
            await interaction.response.send_message(
                f"**host.yaml** (version `{version}`):\n" + "\n".join(lines),
                ephemeral=True,
            )
            return

        # Parse the new value as YAML so true/false/ints work correctly
        try:
            parsed_value = yaml.safe_load(value)
        except Exception:
            parsed_value = value

        # Apply and save
        try:
            path = paths[0]
            old_value = _get_nested(data, path)
            _set_nested(data, path, parsed_value)
            host_yaml_path.write_text(
                yaml.dump(data, default_flow_style=False, allow_unicode=True),
                encoding="utf-8",
            )
            path_str = ".".join(str(k) for k in path)
            log.info(f"/hostyaml {path_str} = {parsed_value!r} (was {old_value!r}) by {interaction.user} (version {version})")
            await interaction.response.send_message(
                f"✅ **host.yaml** updated (version `{version}`):\n`{path_str}`: `{old_value}` → `{parsed_value}`",
                ephemeral=True,
            )
        except Exception as e:
            await interaction.response.send_message(f"⚠️ Failed to update `host.yaml`: `{e}`", ephemeral=True)
