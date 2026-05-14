import asyncio
import logging
from pathlib import Path

import discord

from utils.room_upload import upload_and_create_room
from utils.runs import mark_run_uploaded

log = logging.getLogger('bot')


class SeedSelect(discord.ui.Select):
    def __init__(self, zips_with_counts: list[tuple[Path, int | None]], thread, run_id: str, host: str | None = None):
        self.thread  = thread
        self.run_id  = run_id
        self.host    = host
        options = [
            discord.SelectOption(
                label=p.name[:100],
                description=f"{c} spheres" if c is not None else "sphere count unavailable",
                value=str(p),
            )
            for p, c in zips_with_counts
        ]
        super().__init__(placeholder="Pick a seed to upload…", options=options)

    async def callback(self, interaction: discord.Interaction):
        log.info(f"Seed select callback invoked by {interaction.user}")
        await interaction.response.defer()
        for item in self.view.children:
            item.disabled = True
        await interaction.message.edit(view=self.view)
        self.view.stop()

        from config import ARCHIPELAGO_BASE
        upload_host = self.host or ARCHIPELAGO_BASE
        zip_path = Path(self.values[0])
        await self.thread.send(f"⬆️ Uploading to {upload_host}…")
        try:
            loop = asyncio.get_running_loop()
            room_url = await loop.run_in_executor(None, upload_and_create_room, zip_path, upload_host)
            mark_run_uploaded(self.run_id, zip_path)
            await self.thread.send(f"🎉 Room is ready! <{room_url}>")
        except Exception as e:
            log.exception("Upload failed")
            await self.thread.send(f"⚠️ Upload failed: `{e}`")


class SeedSelectView(discord.ui.View):
    def __init__(self, zips_with_counts: list[tuple[Path, int | None]], thread, run_id: str, host: str | None = None):
        super().__init__(timeout=300)
        self.add_item(SeedSelect(zips_with_counts, thread, run_id, host=host))

    async def on_error(self, interaction: discord.Interaction, error: Exception, item: discord.ui.Item):
        log.exception(f"Error in SeedSelectView item {item}: {error}")
        if not interaction.response.is_done():
            await interaction.response.send_message("⚠️ Something went wrong.", ephemeral=True)
