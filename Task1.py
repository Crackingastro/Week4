import os
import json
import re
import pandas as pd
from telethon import TelegramClient, events
import asyncio
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
phone = os.getenv('PHONE')

# Configuration
MESSAGE_LIMIT = 10
DATA_DIR = f'last_{MESSAGE_LIMIT}_data'
MEDIA_DIR = f'last_{MESSAGE_LIMIT}_media'
CHANNELS = ['ZemenExpress', 'nevacomputer', 'meneshayeofficial', 'ethio_brand_collection', 'Leyueqa']

class Processor:
    def __init__(self):
        self.client = TelegramClient(f'last_{MESSAGE_LIMIT}_scraper', api_id, api_hash)
        
    async def fetch_last_messages(self):
        """Fetch last K messages from each channel"""
        os.makedirs(DATA_DIR, exist_ok=True)
        os.makedirs(MEDIA_DIR, exist_ok=True)
        
        all_messages = []
        
        async with self.client:
            for channel in CHANNELS:
                try:
                    print(f"Fetching last {MESSAGE_LIMIT} messages from @{channel}")
                    messages = await self.client.get_messages(channel, limit=MESSAGE_LIMIT)
                    
                    for message in messages:
                        processed = await self.process_message(message, channel)
                        all_messages.append(processed)
                        
                except Exception as e:
                    print(f"Error fetching from @{channel}: {e}")
        
        # Save to CSV
        df = pd.DataFrame(all_messages)
        df.to_csv('last10_processed.csv', index=False, encoding='utf-8')
        print(f"Saved {len(df)} messages to last10_processed.csv")
        return df

    async def process_message(self, message, channel_name):
        media_path = None
        media_type = None
        file_extension = None
        
        if message.media:
            if isinstance(message.media, MessageMediaPhoto):
                media_type = 'photo'
                file_extension = '.jpg'
            elif isinstance(message.media, MessageMediaDocument):
                media_type = 'document'
                file_extension = self.get_file_extension(message.media.document.mime_type)
            
            if file_extension:
                media_path = await self.download_media(message, file_extension)
        
        message_data = {
            'channel': channel_name,
            'message_id': message.id,
            'date': message.date.isoformat(),
            'text': message.text,
            'views': message.views,
            'media_path': media_path,
            'media_type': media_type,
            'processed_text': self.clean_text(message.text),
            'tokens': self.tokenize_text(message.text)
        }
        
        self.save_message(message_data, channel_name)
        return message_data

    def get_file_extension(self, mime_type):
        """Get proper file extension from mime type"""
        if not mime_type:
            return '.bin'
        return {
            'image/jpeg': '.jpg',
            'image/png': '.png',
            'video/mp4': '.mp4',
            'application/pdf': '.pdf',
            'application/zip': '.zip'
        }.get(mime_type, '.bin')

    async def download_media(self, message, extension):
        """Download media file with proper extension"""
        filename = f"{message.id}_{int(message.date.timestamp())}{extension}"
        filepath = os.path.join(MEDIA_DIR, filename)
        await message.download_media(file=filepath)
        return filepath

    def save_message(self, message_data, channel_name):
        """Save raw message data"""
        filename = f"{channel_name}_{message_data['message_id']}.json"
        with open(os.path.join(DATA_DIR, filename), 'w', encoding='utf-8') as f:
            json.dump(message_data, f, ensure_ascii=False, indent=2)

    def clean_text(self, text):
        if not text:
            return ""
        text = re.sub(r'http\S+|www\S+|https\S+', '', text)
        text = re.sub(r'[^\w\s\u1200-\u137F]', '', text)
        return text.strip()

    def tokenize_text(self, text):
        if not text:
            return []
        return [word for word in re.findall(r'[\w\u1200-\u137F]+', text)]


class LiveChannelMonitor:
    """Live monitoring class for Telegram channels (commented out as requested)"""
    def __init__(self):
        self.client = TelegramClient('live_monitor', api_id, api_hash)
        self.processor = Processor()
    
    async def start_monitoring(self):
        """Start monitoring channels for new messages"""
        async with self.client:
            for channel in CHANNELS:
                self.client.add_event_handler(
                    self.handle_new_message,
                    events.NewMessage(chats=channel)
                )
            print(f"Monitoring started for channels: {', '.join(CHANNELS)}")
            await self.client.run_until_disconnected()
    
    async def handle_new_message(self, event):
        """Process new incoming messages"""
        try:
            message_data = await self.processor.process_message(event.message, event.chat.username)
            print(f"New message from @{event.chat.username}:")
            print(f"Text: {message_data['text']}")
            print(f"Time: {message_data['date']}")
            if message_data['media_path']:
                print(f"Media: {message_data['media_path']}")
        except Exception as e:
            print(f"Error processing new message: {e}")


# Main execution
processor = Processor()
asyncio.run(processor.fetch_last_messages())

# For live monitoring
# monitor = LiveChannelMonitor()
# asyncio.run(monitor.start_monitoring())