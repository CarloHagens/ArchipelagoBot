import discord
from discord.ext import commands

from config import BOT_TOKEN, log

intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
intents.guilds = True


class ArchipelagoBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix=[], intents=intents)

    async def setup_hook(self):
        from cogs.admin import AdminCog
        from cogs.generation import GenerationCog
        from cogs.status import StatusCog
        from cogs.gather import GatherCog
        from cogs.output import OutputCog
        from cogs.scheduling import SchedulingCog
        from cogs.monitor import MonitorCog

        await self.add_cog(AdminCog(self))
        await self.add_cog(GenerationCog(self))
        await self.add_cog(StatusCog(self))
        await self.add_cog(GatherCog(self))
        await self.add_cog(OutputCog(self))
        await self.add_cog(SchedulingCog(self))
        await self.add_cog(MonitorCog(self))
        await self.tree.sync()
        log.info("Cogs loaded and command tree synced.")


bot = ArchipelagoBot()
bot.run(BOT_TOKEN, log_handler=None)
