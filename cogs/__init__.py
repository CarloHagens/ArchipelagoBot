import discord
from discord import app_commands

from config import SERVER_PASSWORD


def is_thread(interaction: discord.Interaction) -> bool:
    return isinstance(interaction.channel, discord.Thread)


def build_generation_opts(
    server_password: str | None,
    release:   app_commands.Choice[str] | None,
    collect:   app_commands.Choice[str] | None,
    remaining: app_commands.Choice[str] | None,
    spoiler:   app_commands.Choice[str] | None,
    race:      str | None,
    password:  str | None,
) -> dict:
    opts = {"server_password": server_password or SERVER_PASSWORD}
    if release:        opts["release_mode"]   = release.value
    if collect:        opts["collect_mode"]   = collect.value
    if remaining:      opts["remaining_mode"] = remaining.value
    if spoiler:        opts["spoiler"]        = int(spoiler.value)
    if race == "yes":  opts["race"]           = 1
    if password:       opts["password"]       = password
    return opts
