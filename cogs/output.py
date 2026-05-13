import logging
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands

from cogs import is_thread
from utils.autocomplete import run_autocomplete, seed_autocomplete
from utils.runs import load_runs

log = logging.getLogger('bot')


class OutputCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="output", description="Attach a previously generated seed to this thread")
    @app_commands.describe(run="Generation run to pick from", seed="Seed to attach")
    @app_commands.autocomplete(run=run_autocomplete, seed=seed_autocomplete)
    async def output(self, interaction: discord.Interaction, run: str, seed: str):
        if not is_thread(interaction):
            await interaction.response.send_message("⚠️ This command must be used inside a thread.", ephemeral=True)
            return

        log.info(f"/output invoked by {interaction.user} in #{interaction.channel.name} (run={run})")

        runs     = load_runs()
        run_data = next((r for r in runs if r["id"] == run), None)
        if not run_data:
            await interaction.response.send_message("⚠️ Run not found.", ephemeral=True)
            return

        zip_path = Path(seed)
        if not zip_path.exists():
            await interaction.response.send_message("⚠️ Zip file no longer exists on disk.", ephemeral=True)
            return

        await interaction.response.send_message(f"📦 Attaching `{zip_path.name}`…")
        try:
            await interaction.channel.send(file=discord.File(zip_path))
        except discord.HTTPException as e:
            if e.status == 413:
                await interaction.channel.send("⚠️ File is too large to attach for this server.")
            else:
                await interaction.channel.send(f"⚠️ Failed to attach file: `{e}`")
