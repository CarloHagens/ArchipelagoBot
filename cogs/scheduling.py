import asyncio
import logging
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands

import state
from cogs import build_generation_opts, is_thread
from config import (
    MAX_SEEDS_PER_RUN, TIMEZONE,
    VALID_RELEASE_COLLECT_MODES, VALID_REMAINING_MODES, SPOILER_MODES, HOST_OPTIONS,
)
from utils.autocomplete import time_autocomplete, timezone_autocomplete, version_autocomplete
from utils.generation import execute_generation
from utils.schedule_helpers import (
    get_scheduled_job, load_scheduled, parse_schedule_time, remove_scheduled_job, save_scheduled,
)
from utils.thread_collector import collect_files_from_thread
from utils.versions import get_installed_versions, get_version_dir

log = logging.getLogger('bot')


class SchedulingCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self):
        state.scheduled[:] = load_scheduled()
        versions = get_installed_versions()
        log.info(f"Logged in as {self.bot.user} — slash commands synced via setup_hook.")
        log.info(f"Installed Archipelago versions: {versions if versions else 'none yet'}")
        log.info(f"{len(state.scheduled)} generation(s) scheduled. Server timezone: {TIMEZONE}")
        if state.checker_task and not state.checker_task.done():
            state.checker_task.cancel()
            try:
                await state.checker_task
            except asyncio.CancelledError:
                pass
        state.checker_task = asyncio.create_task(self._schedule_checker_loop())

    async def _schedule_checker_loop(self) -> None:
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                await self._check_due_schedules()
            except Exception:
                log.exception("Error in schedule checker loop")
            await asyncio.sleep(30)

    async def _check_due_schedules(self) -> None:
        if not state.scheduled:
            return
        now = datetime.now(timezone.utc)
        due = [j for j in state.scheduled if datetime.fromisoformat(j["scheduled_utc"]) <= now]
        if not due:
            return
        due_ids = {j["thread_id"] for j in due}
        state.scheduled[:] = [j for j in state.scheduled if j["thread_id"] not in due_ids]
        save_scheduled()
        for job in due:
            log.info(f"Firing scheduled generation for thread {job['thread_id']} ({job['thread_name']})")
            try:
                thread = await self.bot.fetch_channel(job["thread_id"])
                await self._run_scheduled_generate(thread, job)
            except Exception:
                log.exception(f"Failed to fire scheduled job for thread {job['thread_id']}")

    async def _run_scheduled_generate(self, thread: discord.Thread, job: dict) -> None:
        reserved_bytes = 0
        try:
            versions = get_installed_versions()
            if not versions:
                await thread.send("⚠️ Scheduled generation failed: no Archipelago versions installed.")
                return

            version = job.get("version") or versions[0]
            version_dir = get_version_dir(version)
            if not version_dir.exists():
                await thread.send(f"⚠️ Scheduled generation failed: version `{version}` is no longer installed.")
                return

            count = job.get("count", 1)
            await thread.send(f"⏰ Running scheduled generation with Archipelago `{version}`…")

            scan = await collect_files_from_thread(thread, self.bot.user)
            reserved_bytes = scan.reserved_bytes

            if not scan.yaml_data:
                if not scan.had_error:
                    await thread.send("⚠️ Scheduled generation: no YAML files found in this thread.")
                return

            seed_label = "seed" if count == 1 else f"{count} seeds"
            await thread.send(f"⚙️ Found **{len(scan.yaml_data)}** yaml(s) and **{len(scan.apworld_data)}** apworld(s). Generating {seed_label}… this may take a minute.")

            await execute_generation(
                self.bot.user, thread, job.get("opts", {}), version_dir,
                scan.yaml_data, scan.apworld_data, scan.yaml_uploaders, count,
                host=job.get("host"),
            )

        except Exception:
            log.exception(f"Error in scheduled generation for thread {thread.id}")
            try:
                await thread.send("⚠️ Scheduled generation encountered an unexpected error. Check the bot logs.")
            except Exception:
                pass
        finally:
            state.memory_in_use -= reserved_bytes

    @app_commands.command(name="schedule", description="Schedule a generation for this thread — uses whatever files are posted when the time comes")
    @app_commands.describe(
        time="When to generate — e.g. 'friday 8pm', 'in 2 hours', '2026-05-15 20:00'",
        timezone="Your timezone, e.g. 'Europe/London'. Overrides server default. Current default: " + TIMEZONE,
        cancel="Cancel the scheduled generation for this thread",
        release="When players can release remaining items (default: auto)",
        collect="When players can collect remaining items (default: auto)",
        remaining="When players can query remaining items (default: goal)",
        spoiler="Spoiler log detail level (default: full)",
        race="Enable race mode",
        password="Server join password, only visible to you (optional)",
        server_password="Admin password, overrides default, only visible to you (optional)",
        version="Archipelago version to generate with (default: latest)",
        count=f"Number of seeds to generate (default: 1, max: {MAX_SEEDS_PER_RUN})",
        host="Where to host the room (default: archipelago.gg)",
    )
    @app_commands.choices(
        cancel=[app_commands.Choice(name="yes", value="yes")],
        release=[app_commands.Choice(name=m, value=m) for m in VALID_RELEASE_COLLECT_MODES],
        collect=[app_commands.Choice(name=m, value=m) for m in VALID_RELEASE_COLLECT_MODES],
        remaining=[app_commands.Choice(name=m, value=m) for m in VALID_REMAINING_MODES],
        spoiler=[app_commands.Choice(name=name, value=str(val)) for name, val in SPOILER_MODES.items()],
        race=[app_commands.Choice(name="yes", value="yes")],
        host=[app_commands.Choice(name=url, value=url) for url in HOST_OPTIONS],
    )
    @app_commands.autocomplete(time=time_autocomplete, timezone=timezone_autocomplete, version=version_autocomplete)
    async def schedule(
        self,
        interaction: discord.Interaction,
        time: str = None,
        timezone: str = None,
        cancel: str = None,
        release: app_commands.Choice[str] = None,
        collect: app_commands.Choice[str] = None,
        remaining: app_commands.Choice[str] = None,
        spoiler: app_commands.Choice[str] = None,
        race: str = None,
        password: str = None,
        server_password: str = None,
        version: str = None,
        count: int = 1,
        host: str = None,
    ):
        if not is_thread(interaction):
            await interaction.response.send_message("⚠️ This command must be used inside a thread.", ephemeral=True)
            return

        ephemeral = bool(password or server_password)
        thread = interaction.channel

        if cancel == "yes":
            if remove_scheduled_job(thread.id):
                log.info(f"/schedule cancel in #{thread.name} by {interaction.user}")
                await interaction.response.send_message("🗓️ Scheduled generation cancelled.", ephemeral=ephemeral)
            else:
                await interaction.response.send_message("⚠️ No scheduled generation found for this thread.", ephemeral=True)
            return

        if not time:
            existing = get_scheduled_job(thread.id)
            if existing:
                dt = datetime.fromisoformat(existing["scheduled_utc"])
                ts = int(dt.timestamp())
                await interaction.response.send_message(
                    f"🗓️ Generation scheduled for <t:{ts}:F> (<t:{ts}:R>). Use `cancel: yes` to remove it.",
                    ephemeral=True,
                )
            else:
                await interaction.response.send_message(
                    "⚠️ No time provided. Pass a `time` to schedule, or `cancel: yes` to cancel.",
                    ephemeral=True,
                )
            return

        await interaction.response.defer(ephemeral=ephemeral)

        loop = asyncio.get_running_loop()
        dt = await loop.run_in_executor(None, parse_schedule_time, time, timezone)
        if not dt:
            tz_hint = f" (timezone: `{timezone}`)" if timezone else f" (server timezone: `{TIMEZONE}`)"
            await interaction.followup.send(
                f"⚠️ Couldn't parse `{time}` as a future date/time{tz_hint}.",
                ephemeral=True,
            )
            return

        versions = get_installed_versions()
        if version and version not in versions:
            await interaction.followup.send(f"⚠️ Version `{version}` is not installed.", ephemeral=True)
            return

        opts = build_generation_opts(server_password, release, collect, remaining, spoiler, race, password)

        job = {
            "thread_id":     thread.id,
            "thread_name":   thread.name,
            "scheduled_utc": dt.isoformat(),
            "version":       version,
            "count":         max(1, min(count, MAX_SEEDS_PER_RUN)),
            "opts":          opts,
            "host":          host,
        }

        replaced = remove_scheduled_job(thread.id)
        state.scheduled.append(job)
        save_scheduled()

        ts = int(dt.timestamp())
        tz_used = timezone or TIMEZONE
        log.info(f"/schedule in #{thread.name} by {interaction.user}: {dt.isoformat()} (tz={tz_used})")
        suffix = " (replaced previous schedule)" if replaced else ""
        await interaction.followup.send(
            f"🗓️ Generation scheduled for <t:{ts}:F> (<t:{ts}:R>){suffix}.",
            ephemeral=ephemeral,
        )
