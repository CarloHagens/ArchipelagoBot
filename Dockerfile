FROM python:3.13-slim

RUN apt-get update -qq && apt-get install -y -qq git tk && rm -rf /var/lib/apt/lists/*

WORKDIR /bot

RUN pip install --quiet discord.py requests pyyaml

COPY archipelago_bot.py version_manager.py ./
