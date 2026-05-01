# Archipelago Discord Bot

A Discord bot that generates and hosts [Archipelago](https://archipelago.gg) multiworld randomizer sessions directly from a Discord thread.

## How it works

Members post their `.yaml` config files (and optionally `.apworld` files) in a Discord thread. The bot collects them, runs the Archipelago generator, uploads the result to archipelago.gg, and posts the room link back in the thread.

## Setup

### 1. Create a Discord bot

1. Go to the [Discord Developer Portal](https://discord.com/developers/applications) and create a new application.
2. Under **Bot**, enable the **Message Content Intent** under Privileged Gateway Intents.
3. Copy your bot token — you'll need it when running the container.

### 2. Invite the bot to your server

In **OAuth2 → URL Generator**, select the `bot` and `applications.commands` scopes, then grant these permissions:

| Permission | Why |
|---|---|
| Read Messages / View Channels | Access threads |
| Send Messages | Post status updates and results |
| Send Messages in Threads | Post inside the active thread |
| Read Message History | Scan the thread for uploaded files |

### 3. Deploy with Docker

Build and push the image:

```bash
docker build -t youruser/archipelago-bot:latest .
docker push youruser/archipelago-bot:latest
```

On your host, set the following environment variables and start the container:

| Variable | Description |
|---|---|
| `BOT_TOKEN` | Your bot token from the Discord Developer Portal |
| `SERVER_PASSWORD` | Default admin password for generated rooms (defaults to `archipelago`) |

```bash
BOT_TOKEN=your_token_here SERVER_PASSWORD=yourpassword docker compose up -d
```

The Archipelago source is cloned automatically on first start into a named volume and persists across container restarts.

### Updating

After making changes to `archipelago_bot.py`, rebuild and push, then pull and recreate on your host:

```bash
# On your build machine
docker build -t youruser/archipelago-bot:latest .
docker push youruser/archipelago-bot:latest

# On your host
docker compose pull
docker compose up -d
```

The `archipelago_src` volume is preserved — no need to re-clone Archipelago or re-copy ROMs.

## Slash commands

| Command | Description |
|---|---|
| `/generate` | Collect files from the thread, generate the multiworld, and post the room link |
| `/status` | List the yaml and apworld files found in the current thread |

`/generate` has optional parameters:

| Parameter | Options | Default |
|---|---|---|
| `release` | `disabled`, `enabled`, `auto`, `auto-enabled`, `goal` | `auto` |
| `collect` | `disabled`, `enabled`, `auto`, `auto-enabled`, `goal` | `auto` |
| `remaining` | `disabled`, `enabled`, `goal` | `goal` |
| `spoiler` | `0` (none), `1` (basic), `2` (playthrough), `3` (full) | `3` |
| `race` | `true` / `false` | `false` |
| `password` | Any string — only visible to you | *(none)* |
| `server_password` | Overrides the default admin password — only visible to you | *(none)* |
