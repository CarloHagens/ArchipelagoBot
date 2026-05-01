FROM python:3.13-slim

RUN apt-get update -qq && apt-get install -y -qq git && rm -rf /var/lib/apt/lists/*

WORKDIR /bot

RUN pip install --quiet discord.py requests pyyaml

COPY archipelago_bot.py .

CMD ["bash", "-c", "\
  if [ ! -f /archipelago/Generate.py ]; then \
    git clone --depth 1 https://github.com/ArchipelagoMW/Archipelago.git /archipelago && \
    pip install --quiet -r /archipelago/requirements.txt || true; \
  fi && \
  python archipelago_bot.py"]
