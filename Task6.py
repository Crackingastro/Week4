import os
from dotenv import load_dotenv
from telethon import TelegramClient, events
from datetime import datetime, timedelta
import pandas as pd
import asyncio

# Load environment variables
load_dotenv()

# Telegram API credentials
api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
phone = os.getenv('PHONE')

# Channels to analyze
CHANNELS = ['ZemenExpress', 'nevacomputer', 'meneshayeofficial', 'ethio_brand_collection', 'Leyueqa']

async def analyze_vendor_performance():
    client = TelegramClient('session_name', api_id, api_hash)
    await client.start(phone)
    
    print("Connected to Telegram. Starting analysis...")
    
    results = []
    
    for channel in CHANNELS:
        print(f"\nAnalyzing channel: {channel}")
        
        try:
            entity = await client.get_entity(channel)
            
            messages = []
            async for message in client.iter_messages(entity, limit=500):  # Adjust limit as needed
                messages.append(message)
            
            if not messages:
                print(f"No messages found for {channel}")
                continue
            
            df = pd.DataFrame([{
                'date': msg.date,
                'views': msg.views if msg.views else 0,
                'text': msg.text,
                'channel': channel
            } for msg in messages if msg])
            
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            
            min_date = df['date'].min()
            max_date = df['date'].max()
            days_active = (max_date - min_date).days
            weeks_active = max(days_active / 7, 1)  
            
            total_posts = len(df)
            posts_per_week = total_posts / weeks_active
            
            avg_views = df['views'].mean()
            max_views = df['views'].max()
            top_post = df[df['views'] == max_views].iloc[0]
            
            product = "Unknown"
            price = "Not specified"
            
            if pd.notna(top_post['text']):
                text = top_post['text']
                import re
                price_match = re.search(r'(?:[\$€£¥]|ETB|Birr)\s*(\d+[.,]?\d*)', text)
                if price_match:
                    price = price_match.group(0)
                
                product = text.split('\n')[0] if '\n' in text else ' '.join(text.split()[:10])
            
            lending_score = (avg_views * 0.5) + (posts_per_week * 0.5)
            
            results.append({
                'Channel': channel,
                'Total Posts': total_posts,
                'Time Period (days)': days_active,
                'Posting Frequency (posts/week)': round(posts_per_week, 2),
                'Avg Views per Post': round(avg_views),
                'Top Post Views': max_views,
                'Top Post Product': product[:100] + '...' if len(product) > 100 else product,
                'Top Post Price': price,
                'Lending Score': round(lending_score, 2)
            })
            
            print(f"Analysis complete for {channel}")
            
        except Exception as e:
            print(f"Error analyzing {channel}: {str(e)}")
            continue
    
    print("\nVendor Performance Analysis:")
    results_df = pd.DataFrame(results)
    print(results_df.to_string(index=False))
    
    # Save to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = "vendor_analytics.csv"
    results_df.to_csv(filename, index=False)
    print(f"\nResults saved to {filename}")

# Run the analysis
if __name__ == '__main__':
    asyncio.run(analyze_vendor_performance())