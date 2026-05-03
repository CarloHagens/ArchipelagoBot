# Archipelago Discord Bot

A Discord bot that generates and hosts [Archipelago](https://archipelago.gg) multiworld randomizer sessions directly from a Discord thread.

## How it works

Members post their `.yaml` config files (and optionally `.apworld` files) in a Discord thread. The bot collects them, runs the Archipelago generator, uploads the result to archipelago.gg, and posts the room link back in the thread.

Archipelago versions are managed automatically — the two latest stable releases are kept installed and checked for updates every 12 hours.

## Setup

### 1. Create a Discord bot

1. Go to the [Discord Developer Portal](https://discord.com/developers/applications) and create a new application.
2. Under **Bot**, enable the **Message Content Intent** under Privileged Gateway Intents.
3. Copy your bot token — you'll need it when deploying.

### 2. Invite the bot to your server

In **OAuth2 → URL Generator**, select the `bot` and `applications.commands` scopes, then grant these permissions:

| Permission | Why |
|---|---|
| Read Messages / View Channels | Access threads |
| Send Messages | Post status updates and results |
| Send Messages in Threads | Post inside the active thread |
| Read Message History | Scan the thread for uploaded files |

### 3. Prepare folders on your host

Create two folders on your host machine before starting the containers — Docker requires them to exist:

```
/your/archipelago/path/   ← Archipelago versions, logs, and generated zips
/your/roms/path/          ← ROM files (optional, only needed for certain games)
```

### 4. Deploy with Docker

Build and push the image from your build machine:

```bash
docker build -t youruser/archipelago-bot:latest .
docker push youruser/archipelago-bot:latest
```

On your host, update the volume paths in `docker-compose.yml` to match the folders you created, fill in your environment variables, and start the containers:

```bash
docker compose up -d
```

Two containers will start:
- **archipelago-bot** — the Discord bot
- **archipelago-versions** — downloads and manages Archipelago releases automatically

On first start, `archipelago-versions` will clone the two latest stable Archipelago releases. This may take a few minutes before the bot is ready to generate.

### Environment variables

| Variable | Description |
|---|---|
| `BOT_TOKEN` | Your bot token from the Discord Developer Portal |
| `SERVER_PASSWORD` | Default admin password for generated rooms (defaults to `archipelago`) |

### Updating the bot

After making changes, rebuild and push from your build machine, then pull and recreate on your host:

```bash
# Build machine
docker build -t youruser/archipelago-bot:latest .
docker push youruser/archipelago-bot:latest

# Host
docker compose pull
docker compose up -d
```

Archipelago versions and any ROM files are preserved across updates.

## Slash commands

| Command | Description |
|---|---|
| `/generate` | Collect files from the thread, generate the multiworld, and post the room link |
| `/status` | List the yaml and apworld files found in the current thread |
| `/last_output` | Attach the most recently generated zip to the thread |
| `/validate` | Validate all YAML files in the thread against archipelago.gg |

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
| `version` | Installed Archipelago version to use | latest |
| `dry_run` | Generate locally without uploading to archipelago.gg | `false` |
