import logging

import discord
from discord import app_commands
from discord.ext import commands

import state
from cogs import is_thread
from state import get_audit_lock
from utils.thread_collector import audit_thread
from utils.yaml_validation import count_yaml_players

log = logging.getLogger('bot')


class StatusCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="status", description="List files found in this thread and validate YAMLs against archipelago.gg")
    async def status(self, interaction: discord.Interaction):
        if not is_thread(interaction):
            await interaction.response.send_message("⚠️ This command must be used inside a thread.", ephemeral=True)
            return

        log.info(f"/status invoked by {interaction.user} in #{interaction.channel.name}")
        await interaction.response.send_message("🔍 Scanning and validating thread…")
        thread = interaction.channel

        async with get_audit_lock(thread.id):
            result = await audit_thread(thread, self.bot.user)
            try:
                yaml_list    = ", ".join(f"`{f}`" for f in result.yaml_data)    or "none"
                apworld_list = ", ".join(f"`{f}`" for f in result.apworld_data) or "none"
                total_yamls  = sum(count_yaml_players(d) for d in result.yaml_data.values())
                msg = (
                    f"**Files found in this thread:**\n"
                    f"📄 **YAMLs ({total_yamls}):** {yaml_list}\n"
                    f"🌍 **APworlds ({len(result.apworld_data)}):** {apworld_list}"
                )
                if result.issues:
                    issue_lines = "\n".join(display for _, display in result.issues)
                    msg += f"\n\n⚠️ **Issues:**\n{issue_lines}"
                else:
                    msg += "\n\n✅ No issues found."
                await thread.send(msg)
            finally:
                state.memory_in_use -= result.reserved_bytes
