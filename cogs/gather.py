import io
import logging
import re

import discord
from discord import app_commands
from discord.ext import commands

import state
from cogs import is_thread
from utils.files import _build_gather_zip
from utils.thread_collector import collect_files_from_thread

log = logging.getLogger('bot')


class GatherCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="gather", description="Collect all YAMLs and apworlds from this thread and attach them as a zip")
    async def gather(self, interaction: discord.Interaction):
        if not is_thread(interaction):
            await interaction.response.send_message("⚠️ This command must be used inside a thread.", ephemeral=True)
            return

        log.info(f"/gather invoked by {interaction.user} in #{interaction.channel.name}")
        await interaction.response.send_message("📦 Gathering files…")
        thread = interaction.channel

        scan = await collect_files_from_thread(thread, self.bot.user, audit=True)
        try:
            if not scan.yaml_data and not scan.apworld_data:
                msg = "⚠️ No files found in this thread."
                if scan.issues:
                    msg += " Run `/status` for details on skipped files."
                await thread.send(msg)
                return

            summary = f"📄 {len(scan.yaml_data)} YAML(s)  🌍 {len(scan.apworld_data)} apworld(s)"
            if scan.issues:
                summary += " — ⚠️ some files were skipped, run `/status` for details."

            safe_name = re.sub(r'[^\w\-. ]', '_', thread.name)[:50].strip()

            combined = _build_gather_zip(scan.yaml_data, scan.apworld_data)
            try:
                await thread.send(summary, file=discord.File(io.BytesIO(combined), filename=f"{safe_name}.zip"))
                return
            except discord.HTTPException as e:
                if e.status != 413:
                    raise

            await thread.send(f"{summary}\n📦 Combined zip too large — sending as separate files:")

            async def send_or_split(zip_name: str, yaml_d: dict[str, bytes], apworld_d: dict[str, bytes]) -> None:
                data = _build_gather_zip(yaml_d, apworld_d)
                try:
                    await thread.send(file=discord.File(io.BytesIO(data), filename=zip_name))
                    return
                except discord.HTTPException as e:
                    if e.status != 413:
                        raise
                individual_files = {**yaml_d, **apworld_d}
                await thread.send(f"📦 `{zip_name}` too large — sending {len(individual_files)} file(s) individually:")
                for fname, fdata in individual_files.items():
                    try:
                        await thread.send(file=discord.File(io.BytesIO(fdata), filename=fname))
                    except discord.HTTPException as e:
                        if e.status == 413:
                            await thread.send(f"⚠️ `{fname}` is too large to attach.")
                        else:
                            raise

            if scan.yaml_data:
                await send_or_split("players.zip", scan.yaml_data, {})
            if scan.apworld_data:
                await send_or_split("custom_worlds.zip", {}, scan.apworld_data)
        finally:
            state.memory_in_use -= scan.reserved_bytes
