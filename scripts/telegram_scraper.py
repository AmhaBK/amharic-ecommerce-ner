from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto
import csv
import os
from dotenv import load_dotenv
from datetime import datetime
import re

# Load environment variables
load_dotenv('.env')
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')
phone = os.getenv('phone')

# Clean text utility
def clean_text(text):
    if not text:
        return ''
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

# Scrape function
async def scrape_channel(client, channel_username, writer, media_dir):
    try:
        entity = await client.get_entity(channel_username)
        channel_title = entity.title
        async for message in client.iter_messages(entity, limit=1000):
            media_path = None

            if message.media and isinstance(message.media, MessageMediaPhoto):
                filename = f"{channel_username}_{message.id}.jpg"
                media_path = os.path.join(media_dir, filename)
                await client.download_media(message.media, media_path)

            cleaned_text = clean_text(message.message)
            views = message.views if hasattr(message, 'views') else None
            forwards = message.forwards if hasattr(message, 'forwards') else None
            reply_count = message.replies.replies if message.replies else None

            writer.writerow([
                channel_title,
                channel_username,
                message.id,
                cleaned_text,
                message.date.strftime("%Y-%m-%d %H:%M:%S"),
                media_path,
                views,
                forwards,
                reply_count
            ])
    except Exception as e:
        print(f"Error scraping {channel_username}: {e}")

# Telegram client setup
client = TelegramClient('scraping_session', api_id, api_hash)

# Main logic
async def main():
    await client.start()

    media_dir = 'photos'
    os.makedirs(media_dir, exist_ok=True)

    with open('telegram_data.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([
            'Channel Title', 'Channel Username', 'Message ID', 'Message',
            'Date', 'Media Path', 'Views', 'Forwards', 'Replies'
        ])

        channels = [
            '@ethio_brand_collection',
            '@qnashcom',
            '@Leyueqa',
            '@ZemenExpress',
            '@sinayelj'
        ]

        for channel in channels:
            await scrape_channel(client, channel, writer, media_dir)
            print(f"✅ Finished scraping {channel}")

with client:
    client.loop.run_until_complete(main())