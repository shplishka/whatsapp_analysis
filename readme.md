# Message Processing and Analysis System

This system processes WhatsApp messages, extracts structured information using Claude API, and stores the results in PostgreSQL. It consists of three main components:
- Message parser and formatter
- Database loader
- PostgreSQL database

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Anthropic API key

## Project Structure

```
message-analysis/
├── docker/
│   └── docker-compose.yml
├── config/
│   ├── config.json
│   └── schema.json
├── scripts/
│   ├── whatsapp_parser.py
│   └── db_loader.py
├── requirements.txt
└── README.md
```

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd message-analysis
```

2. Create a Python virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate  
pip install -r requirements.txt
```

3. Create `docker/docker-compose.yml`:
```yaml
version: '3.8'

services:
  postgres:
    image: postgres:14
    environment:
      POSTGRES_DB: mishka
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: yourpassword
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
```

4. Create `config/config.json`:
```json
{
    "database": {
        "host": "localhost",
        "port": 5432,
        "database": "mishka",
        "user": "postgres",
        "password": "yourpassword"
    }
}
```

5. Create `config/schema.json`:
```json
{
    "system_prompt": "You are an expert at analyzing IT incident messages in Hebrew. Extract structured information from the message.",
    "output_format": {
        "incident_id": {
            "type": "string",
            "description": "Incident/SN number if present"
        },
        "severity": {
            "type": "string",
            "enum": ["critical", "high", "medium", "low"],
            "description": "Severity level"
        },
        "service_affected": {
            "type": "string",
            "description": "Name of affected service"
        },
        "platform": {
            "type": "string",
            "description": "Platform name"
        },
        "teams": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Teams involved"
        },
        "impact": {
            "type": "string",
            "description": "Impact description"
        },
        "action_taken": {
            "type": "string",
            "description": "Actions being taken"
        },
        "type": {
            "type": "string",
            "description": "Type of incident"
        }
    }
}
```

## Starting the System

1. Start PostgreSQL:
```bash
cd docker
docker-compose up -d
```

2. Set your Anthropic API key:
```bash
# On Linux/Mac
export ANTHROPIC_API_KEY=your-api-key-here
```

## Processing Messages

1. Prepare your WhatsApp messages in a text file (e.g., `messages.txt`):
```text
[13/10/2023, 18:36:25] User1: bla bla how r u? 

[13/10/2023, 19:36:23] User2: bro bro bitch 
```

2. Parse and format messages:
```bash
python scripts/whatsapp_parser.py messages.txt config/schema.json
```

This will create a directory `formatted_data_messages` containing:
- Individual JSON files for each message
- A combined CSV file (`messages_formatted.csv`)

3. Load data into PostgreSQL:
```bash
python scripts/db_loader.py ./formatted_data_messages/messages_formatted.csv config/schema.json --config config/config.json
```

## Example Usage

1. Process a WhatsApp chat export:
```bash
# Export chat from WhatsApp and save as chat.txt
python scripts/whatsapp_parser.py messages.txt config/schema.json
```

2. Load formatted data into PostgreSQL:
```bash
python scripts/db_loader.py ./formatted_data_messages/messages_formatted.csv config/schema.json
```

3. Query the data (using psql or any PostgreSQL client):
```sql
-- Connect to database
psql -h localhost -U postgres -d mishka

-- View recent incidents
SELECT incident_id, severity, service_affected, date
FROM problems_chat
ORDER BY date DESC, time DESC
LIMIT 5;

-- Count incidents by severity
SELECT severity, COUNT(*) 
FROM problems_chat 
GROUP BY severity;
```

## Troubleshooting

### Common Issues

1. Database Connection:
```bash
# Check if PostgreSQL is running
docker ps

# View PostgreSQL logs
docker-compose logs postgres
```

2. Data Processing:
```bash
# Check formatted data
head -n 5 ./formatted_data_chat/chat_formatted.csv

# Verify table structure
psql -h localhost -U postgres -d mishka -c "\d problems_chat"
```

### Error Messages

1. "column X does not exist":
   - Verify schema.json matches your data structure
   - Check CSV headers against table columns

2. "connection refused":
   - Ensure PostgreSQL container is running
   - Verify connection settings in config.json

## Customization

### Modifying the Schema

Edit `config/schema.json` to change:
- Field definitions
- Validation rules
- Output format

Example adding a new field:
```json
{
    "output_format": {
        "priority": {
            "type": "string",
            "enum": ["P1", "P2", "P3", "P4"],
            "description": "Incident priority"
        }
    }
}
```

### Database Configuration

Edit `config/config.json` to change:
- Database connection settings
- Table names
- Processing options

## Installing warnAi

As a final step, install warnAi visit:
https://docs.getwren.ai/oss/installation/?utm_source=github&utm_medium=content&utm_campaign=readme

