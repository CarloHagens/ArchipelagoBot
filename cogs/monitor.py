import logging

import discord
from discord import app_commands
from discord.ext import commands

import state
from cogs import is_thread
from config import GITHUB_RELEASE_RE
from state import get_audit_lock
from utils.monitor_helpers import (
    format_resolved, is_monitored, load_monitors, save_monitors, unregister_monitor,
)
from utils.thread_collector import audit_thread

log = logging.getLogger('bot')


def _has_relevant_content(message: discord.Message) -> bool:
    if any(a.filename.lower().endswith((".yaml", ".yml", ".apworld")) for a in message.attachments):
        return True
    return bool(message.content and GITHUB_RELEASE_RE.search(message.content))


def _github_links(message: discord.Message) -> set:
    if not message.content:
        return set()
    return {m.group(0) for m in GITHUB_RELEASE_RE.finditer(message.content)}


class MonitorCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self):
        state.monitors.clear()
        state.monitors.update(load_monitors())
        log.info(f"Monitoring {len(state.monitors)} thread(s).")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author == self.bot.user:
            return
        if not is_monitored(message.channel):
            return
        if not _has_relevant_content(message):
            return
        try:
            await self._check_monitored_thread(message.channel)
        except Exception:
            log.exception(f"Error in on_message monitor check for thread {message.channel.id}")

    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message):
        if not is_monitored(message.channel):
            return
        if not _has_relevant_content(message):
            return
        try:
            await self._check_monitored_thread(message.channel)
        except Exception:
            log.exception(f"Error in on_message_delete monitor check for thread {message.channel.id}")

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        if after.author == self.bot.user:
            return
        if not is_monitored(after.channel):
            return
        attachments_changed = before.attachments != after.attachments
        github_links_changed = _github_links(before) != _github_links(after)
        if not attachments_changed and not github_links_changed:
            return
        if not _has_relevant_content(before) and not _has_relevant_content(after):
            return
        try:
            await self._check_monitored_thread(after.channel)
        except Exception:
            log.exception(f"Error in on_message_edit monitor check for thread {after.channel.id}")

    async def _check_monitored_thread(self, thread: discord.Thread) -> None:
        lock = get_audit_lock(thread.id)
        if lock.locked():
            log.info(f"[monitor #{thread.name}] Scan already in progress — queuing re-check.")
            state.monitor_pending.add(thread.id)
            return
        async with lock:
            while True:
                state.monitor_pending.discard(thread.id)
                log.info(f"[monitor #{thread.name}] Starting scan.")
                result = await audit_thread(thread, self.bot.user)
                try:
                    current     = {key: msg for key, msg in result.issues}
                    entry       = state.monitors.setdefault(str(thread.id), {})
                    known       = set(entry.get("known_issue_keys", []))
                    warning_ids = entry.get("warning_messages", {})

                    new_keys      = [key for key in current if key not in known]
                    resolved_keys = known - current.keys()

                    for key in new_keys:
                        sent = await thread.send(current[key])
                        warning_ids[key] = sent.id
                    if new_keys:
                        log.info(f"[monitor #{thread.name}] {len(new_keys)} new issue(s) found — posting.")

                    for key in resolved_keys:
                        msg_id = warning_ids.pop(key, None)
                        if msg_id:
                            try:
                                warning_msg = await thread.fetch_message(msg_id)
                                await warning_msg.edit(content=format_resolved(warning_msg.content))
                            except Exception:
                                pass
                    if resolved_keys:
                        log.info(f"[monitor #{thread.name}] {len(resolved_keys)} issue(s) resolved — editing warnings.")

                    if not new_keys and not resolved_keys:
                        log.info(f"[monitor #{thread.name}] Scan complete — no changes.")

                    entry["known_issue_keys"] = list(current.keys())
                    entry["warning_messages"] = warning_ids
                    if new_keys or resolved_keys:
                        save_monitors()
                finally:
                    state.memory_in_use -= result.reserved_bytes
                if thread.id not in state.monitor_pending:
                    break
                log.info(f"[monitor #{thread.name}] Re-check was queued during scan — running again.")

    @app_commands.command(name="monitor", description="Start monitoring this thread for issues, or stop if already monitoring")
    async def monitor(self, interaction: discord.Interaction):
        if not is_thread(interaction):
            await interaction.response.send_message("⚠️ This command must be used inside a thread.", ephemeral=True)
            return

        thread     = interaction.channel
        thread_key = str(thread.id)

        if thread_key in state.monitors:
            unregister_monitor(thread.id)
            log.info(f"/monitor: stopped monitoring #{thread.name}")
            await interaction.response.send_message("🔕 Monitoring stopped for this thread.")
            return

        state.monitors[thread_key] = {"known_issue_keys": []}
        save_monitors()
        log.info(f"/monitor: started monitoring #{thread.name}")
        await interaction.response.send_message("🔔 Monitoring started — I'll flag any issues as files are posted.")
        await self._check_monitored_thread(thread)
