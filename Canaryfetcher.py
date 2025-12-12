import discord
import asyncio
import json
import pandas as pd
import sys

# ---------------- CONFIG ----------------
TOKEN_FILE = "token.txt" # a text file containing your Discord bot token
CHANNEL_ID = 1386236984497930312 # copy your specific channel ID
OUTPUT_JSON = "discord_dataset.json" # name of the output file as JSON
OUTPUT_CSV = "discord_dataset.csv" # name of the output file as CSV
# ----------------------------------------

intents = discord.Intents.default()
client = discord.Client(intents=intents)

def parse_json_field(value: str):
    """Strip backticks and parse JSON safely."""
    if not value:
        return None
    value = value.strip().strip("`").strip()
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None

def extract_sections(embed_dict):
    """Extract geo info, headers, useragent from embed fields."""
    record = {}
    headers_data = {}

    if "fields" not in embed_dict:
        return record

    for field in embed_dict["fields"]:
        name = field.get("name", "").lower()
        value = field.get("value", "")
        if not value:
            continue

        if name == "useragent":
            record["useragent"] = value.strip("`").strip()
        elif name == "geo info":
            geo_json = parse_json_field(value)
            if geo_json:
                record.update(geo_json)
        elif name == "request headers":
            headers_json = parse_json_field(value)
            if headers_json:
                headers_data.update(headers_json)

    if headers_data:
        record["headers"] = headers_data

    return record


@client.event
async def on_ready():
    print(f"✅ Logged in as {client.user}")
    channel = client.get_channel(CHANNEL_ID)
    print(f"Fetching messages from: {channel}")

    all_data = []

    async for msg in channel.history(limit=None, oldest_first=True):
        msg_record = {}

        for embed in msg.embeds:
            embed_dict = embed.to_dict()
            parsed = extract_sections(embed_dict)
            msg_record.update(parsed)

        # Only save if geo info is present
        if "ip" in msg_record and "loc" in msg_record:
            all_data.append(msg_record)

    # Save JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    # Save CSV
    df = pd.json_normalize(all_data)
    df.to_csv(OUTPUT_CSV, index=False)

    print(f"\n🎉 Done! Saved {len(all_data)} messages to {OUTPUT_JSON} and {OUTPUT_CSV}")
    await client.close()


if __name__ == "__main__":
    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            TOKEN = f.read().strip()
    except FileNotFoundError:
        print(f"❌ Token file '{TOKEN_FILE}' not found!")
        exit(1)

    client.run(TOKEN)
