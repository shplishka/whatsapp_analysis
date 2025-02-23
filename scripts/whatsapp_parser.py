import json
import asyncio
import aiohttp
import pandas as pd
from pathlib import Path
from typing import Dict, List
import os
import re
from datetime import datetime

class WhatsAppMessageParser:
    def __init__(self, api_key: str, schema_file: str, model: str = "claude-3-sonnet-20240229"):
        self.api_key = api_key
        self.model = model
        
        # Load schema
        with open(schema_file, 'r', encoding='utf-8') as f:
            self.schema = json.load(f)
            
        self.output_dir = None
        
    def parse_timestamp(self, timestamp_str: str) -> tuple:
        """Parse timestamp string into date and time"""
        # Extract timestamp from format [DD/MM/YYYY, HH:MM:SS]
        match = re.match(r'\[(\d{2}/\d{2}/\d{4}), (\d{2}:\d{2}:\d{2})\]', timestamp_str)
        if match:
            date_str, time_str = match.groups()
            return date_str, time_str
        return None, None

    def parse_messages(self, text_content: str) -> List[Dict]:
        """Parse text file content into list of messages"""
        messages = []
        
        # Split text into messages (each starting with timestamp)
        message_pattern = r'(\[\d{2}/\d{2}/\d{4}, \d{2}:\d{2}:\d{2}\].*?)(?=\[\d{2}/\d{2}/\d{4}, \d{2}:\d{2}:\d{2}\]|$)'
        matches = re.finditer(message_pattern, text_content, re.DOTALL)
        
        for match in matches:
            message_text = match.group(1).strip()
            
            # Extract timestamp and actual message
            timestamp_end = message_text.find(']') + 1
            timestamp = message_text[:timestamp_end]
            content = message_text[timestamp_end:].strip()
            
            # Parse timestamp
            date_str, time_str = self.parse_timestamp(timestamp)
            
            if date_str and time_str:
                messages.append({
                    'date': date_str,
                    'time': time_str,
                    'message': content,
                    'message_type': 'text'
                })
        
        return messages

    def setup_output_directory(self, input_path: str):
        """Create output directory based on input filename"""
        input_file = Path(input_path)
        self.output_dir = Path(f"formatted_data_{input_file.stem}")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def format_message(self, message: Dict, message_id: str, session: aiohttp.ClientSession) -> Dict:
        """Format a single message using the schema"""
        try:
            headers = {
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            }
            
            # Build complete prompt with format specification
            format_description = json.dumps(self.schema['output_format'], indent=2)
            prompt = f"""{self.schema['system_prompt']}

Expected output format:
{format_description}

Return ONLY the JSON object matching this format, with no additional text.

Message to analyze:
{message['message']}"""
            
            payload = {
                "model": self.model,
                "max_tokens": 1024,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }

            async with session.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=payload
            ) as response:
                if response.status == 429:  # Rate limit
                    await asyncio.sleep(20)
                    return await self.format_message(message, message_id, session)
                
                result = await response.json()
                
                if "content" in result:
                    try:
                        content = result["content"][0]["text"]
                        formatted_data = json.loads(content)
                        
                        # Add metadata
                        formatted_data.update({
                            "date": message["date"],
                            "time": message["time"],
                            "original_message": message["message"]
                        })
                        
                        return formatted_data
                        
                    except (json.JSONDecodeError, KeyError, IndexError) as e:
                        print(f"Error processing message {message_id}: {str(e)}")
                        return {}
                return {}
                
        except Exception as e:
            print(f"Error formatting message {message_id}: {str(e)}")
            return {}

    def clean_text(self, text):
        """Remove newlines and multiple spaces from text"""
        if isinstance(text, str):
            # Replace newlines with spaces
            text = text.replace('\n', ' ').replace('\r', ' ')
            # Replace multiple spaces with single space
            text = re.sub(r'\s+', ' ', text)
            return text.strip()
        return text

    def save_message_json(self, message_data: Dict, message_id: str):
        """Save formatted message as JSON"""
        filename = self.output_dir / f"message_{message_id}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(message_data, f, ensure_ascii=False, indent=2)

    def save_to_csv(self, formatted_messages: List[Dict], input_name: str):
        """Save all formatted messages to CSV with newlines removed"""
        output_file = self.output_dir / f"{input_name}_formatted.csv"
        
        # Create a copy of the messages to avoid modifying the original data
        cleaned_messages = []
        for message in formatted_messages:
            cleaned_message = {}
            for key, value in message.items():
                if isinstance(value, list):
                    # Clean each item in the list if it's a string
                    cleaned_value = [self.clean_text(item) if isinstance(item, str) else item for item in value]
                    # Join with commas
                    cleaned_message[key] = ','.join(str(item) for item in cleaned_value)
                else:
                    # Clean single value if it's a string
                    cleaned_message[key] = self.clean_text(value) if isinstance(value, str) else value
            cleaned_messages.append(cleaned_message)
        
        # Convert to DataFrame and save
        df = pd.DataFrame(cleaned_messages)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')

    async def process_messages(self, messages: List[Dict], input_path: str):
        """Process multiple messages in batches"""
        self.setup_output_directory(input_path)
        formatted_messages = []
        
        async with aiohttp.ClientSession() as session:
            batch_size = 5
            for i in range(0, len(messages), batch_size):
                batch = messages[i:i + batch_size]
                batch_tasks = []
                
                for msg in batch:
                    # Create a safe filename by replacing invalid characters
                    safe_date = msg['date'].replace('/', '-')
                    safe_time = msg['time'].replace(':', '-')
                    message_id = f"{safe_date}_{safe_time}_{abs(hash(msg['message']))}"
                    task = asyncio.create_task(
                        self.format_message(msg, message_id, session)
                    )
                    batch_tasks.append((message_id, task))
                
                for message_id, task in batch_tasks:
                    try:
                        result = await task
                        if result:
                            self.save_message_json(result, message_id)
                            formatted_messages.append(result)
                    except Exception as e:
                        print(f"Error in batch processing: {str(e)}")
                
                await asyncio.sleep(0.5)
            
            self.save_to_csv(formatted_messages, Path(input_path).stem)

async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Parse and format WhatsApp messages')
    parser.add_argument('input_file', help='Path to input text file')
    parser.add_argument('schema_file', help='Path to JSON schema file')
    args = parser.parse_args()
    
    # Load API key
    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        raise ValueError("Please set ANTHROPIC_API_KEY environment variable")
    
    # Initialize parser
    parser = WhatsAppMessageParser(api_key, args.schema_file)
    
    # Read and parse messages
    with open(args.input_file, 'r', encoding='utf-8') as f:
        text_content = f.read()
    
    messages = parser.parse_messages(text_content)
    print(f"Found {len(messages)} messages to process")
    
    # Process messages
    await parser.process_messages(messages, args.input_file)
    print(f"Processed data saved in {parser.output_dir}/")

if __name__ == "__main__":
    asyncio.run(main())