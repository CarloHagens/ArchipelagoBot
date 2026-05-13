import logging

import discord
from discord import app_commands
from discord.ext import commands

import state
from config import (
    MAX_SEEDS_PER_RUN, SERVER_PASSWORD,
    VALID_RELEASE_COLLECT_MODES, VALID_REMAINING_MODES, SPOILER_MODES,
)
from utils.autocomplete import version_autocomplete
from utils.generation import execute_generation
from utils.versions import get_installed_versions, get_version_dir
from utils.thread_collector import collect_files_from_thread

log = logging.getLogger('bot')


def is_thread(interaction: discord.Interaction) -> bool:
    return isinstance(interaction.channel, discord.Thread)


class GenerationCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="generate", description="Generate and host an Archipelago multiworld from this thread's files")
    @app_commands.describe(
        release="When players can release remaining items from their world (default: auto)",
        collect="When players can collect remaining items into their world (default: auto)",
        remaining="When players can query remaining items via !remaining (default: goal)",
        spoiler="Spoiler log detail level (default: full)",
        race="Enable race mode",
        password="Server join password, only visible to you (optional)",
        server_password="Admin password, overrides default, only visible to you (optional)",
        version="Archipelago version to generate with (default: latest)",
        dry_run="Generate locally without uploading to archipelago.gg",
        count=f"Number of seeds to generate (default: 1, max: {MAX_SEEDS_PER_RUN})",
    )
    @app_commands.choices(
        release=[app_commands.Choice(name=m, value=m) for m in VALID_RELEASE_COLLECT_MODES],
        collect=[app_commands.Choice(name=m, value=m) for m in VALID_RELEASE_COLLECT_MODES],
        remaining=[app_commands.Choice(name=m, value=m) for m in VALID_REMAINING_MODES],
        spoiler=[app_commands.Choice(name=name, value=str(val)) for name, val in SPOILER_MODES.items()],
        race=[app_commands.Choice(name="yes", value="yes")],
        dry_run=[app_commands.Choice(name="yes", value="yes")],
    )
    @app_commands.autocomplete(version=version_autocomplete)
    async def generate(
        self,
        interaction: discord.Interaction,
        release: app_commands.Choice[str] = None,
        collect: app_commands.Choice[str] = None,
        remaining: app_commands.Choice[str] = None,
        spoiler: app_commands.Choice[str] = None,
        race: str = None,
        password: str = None,
        server_password: str = None,
        version: str = None,
        dry_run: str = None,
        count: int = 1,
    ):
        if not is_thread(interaction):
            await interaction.response.send_message("⚠️ This command must be used inside a thread.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        log.info(f"/generate invoked by {interaction.user} in #{interaction.channel.name} (version={version or 'latest'}, count={count}, dry_run={dry_run})")
        reserved_bytes = 0
        try:
            versions = get_installed_versions()
            if not versions:
                await interaction.followup.send("⚠️ No Archipelago versions are installed yet. Please wait for the version manager to finish.", ephemeral=True)
                return

            version     = version or versions[0]
            version_dir = get_version_dir(version)
            if not version_dir.exists():
                await interaction.followup.send(f"⚠️ Version `{version}` is not installed.", ephemeral=True)
                return

            count = max(1, min(count, MAX_SEEDS_PER_RUN))
            await interaction.followup.send(f"⏳ Starting {'generation' if count == 1 else f'{count} generations'} with Archipelago `{version}`…", ephemeral=True)
            thread = interaction.channel
            await thread.send("🔍 Scanning thread history for files...")

            try:
                scan = await collect_files_from_thread(thread, self.bot.user)
            except discord.HTTPException as e:
                log.warning(f"Discord API error while scanning thread history: {e}")
                try:
                    await thread.send(f"⚠️ Discord API error while scanning thread history: `{e}`. Please try again.")
                except discord.HTTPException:
                    pass
                return
            reserved_bytes = scan.reserved_bytes
            yaml_data, apworld_data, yaml_uploaders = scan.yaml_data, scan.apworld_data, scan.yaml_uploaders
            log.info(f"Collected {len(yaml_data)} yaml(s) and {len(apworld_data)} apworld(s) from thread.")
            if not yaml_data:
                if not scan.had_error:
                    await thread.send("⚠️ No YAML files found in this thread — nothing to generate.")
                return

            seed_label = "seed" if count == 1 else f"{count} seeds"
            await thread.send(f"⚙️ Found **{len(yaml_data)}** yaml(s) and **{len(apworld_data)}** apworld(s). Generating {seed_label}… this may take a minute.")

            gen_opts: dict = {"server_password": server_password or SERVER_PASSWORD}
            if release:   gen_opts["release_mode"]   = release.value
            if collect:   gen_opts["collect_mode"]   = collect.value
            if remaining: gen_opts["remaining_mode"] = remaining.value
            if spoiler:   gen_opts["spoiler"]        = int(spoiler.value)
            if race:      gen_opts["race"]           = 1
            if password:  gen_opts["password"]       = password

            await execute_generation(self.bot.user, thread, gen_opts, version_dir, yaml_data, apworld_data, yaml_uploaders, count, dry_run=dry_run == "yes")

        finally:
            state.memory_in_use -= reserved_bytes
