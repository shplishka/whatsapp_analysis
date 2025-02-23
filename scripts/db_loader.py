import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
import argparse
from pathlib import Path
import json
import re
from typing import Dict, Any, List, Optional
from datetime import datetime

class SchemaParser:
    def __init__(self, schema_path: str):
        with open(schema_path, 'r', encoding='utf-8') as f:
            self.schema = json.load(f)
    
    def get_field_definitions(self) -> Dict[str, Dict]:
        metadata_fields = {
            "date": {
                "type": "string",
                "format": "date",
                "required": True
            },
            "time": {
                "type": "string",
                "format": "time",
                "required": True
            },
            "original_message": {
                "type": "string",
                "required": True
            }
        }
        
        if 'output_format' in self.schema:
            fields = self.schema['output_format']
        elif 'properties' in self.schema:
            fields = self.schema['properties']
        elif 'fields' in self.schema:
            fields = self.schema['fields']
        else:
            fields = {k: v for k, v in self.schema.items() 
                     if isinstance(v, dict) and 'type' in v}
        
        return {**metadata_fields, **fields}

    def get_field_type(self, field_info: Dict) -> str:
        if isinstance(field_info, dict):
            return field_info.get('type', 'string').lower()
        return 'string'

    def is_required(self, field_name: str, field_info: Dict) -> bool:
        if 'required' in field_info:
            return field_info['required']
        elif 'required' in self.schema:
            return field_name in self.schema['required']
        return False

    def get_enum_values(self, field_info: Dict) -> Optional[List[str]]:
        if isinstance(field_info, dict):
            return field_info.get('enum', None)
        return None

class DatabaseTypeMapper:
    TYPE_MAPPING = {
        'string': 'TEXT',
        'number': 'NUMERIC',
        'integer': 'INTEGER',
        'boolean': 'BOOLEAN',
        'array': 'TEXT',
        'object': 'JSONB',
        'date': 'DATE',
        'time': 'TIME',
        'datetime': 'TIMESTAMP'
    }
    
    @classmethod
    def get_sql_type(cls, field_type: str, field_info: Dict) -> str:
        base_type = field_type.lower()
        
        if 'enum' in field_info:
            return 'VARCHAR(50)'
        
        if base_type == 'array':
            item_type = field_info.get('items', {}).get('type', 'string').lower()
            if item_type in cls.TYPE_MAPPING:
                return f"{cls.TYPE_MAPPING[item_type]}[]"
            return 'TEXT[]'
        
        if base_type == 'string' and 'maxLength' in field_info:
            return f"VARCHAR({field_info['maxLength']})"
        
        return cls.TYPE_MAPPING.get(base_type, 'TEXT')

class GenericDatabaseLoader:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.engine = create_engine(
            f"postgresql://{self.config['database']['user']}:{self.config['database']['password']}"
            f"@{self.config['database']['host']}:{self.config['database']['port']}"
            f"/{self.config['database']['database']}"
        )
    
    def create_table_from_schema(self, table_name: str, schema_parser: SchemaParser) -> str:
        field_definitions = schema_parser.get_field_definitions()
        print("\nProcessing schema fields:")
        for field_name, field_info in field_definitions.items():
            print(f"Field: {field_name}, Info: {field_info}")
        
        columns = [
            "id SERIAL PRIMARY KEY",
            "date DATE",
            "time TIME",
            "original_message TEXT",
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        ]
        
        for field_name, field_info in field_definitions.items():
            if field_name in ['date', 'time', 'original_message']:
                continue
                
            field_type = schema_parser.get_field_type(field_info)
            sql_type = DatabaseTypeMapper.get_sql_type(field_type, field_info)
            
            nullable = "" if schema_parser.is_required(field_name, field_info) else " NULL"
            columns.append(f"{field_name} {sql_type}{nullable}")
        
        sql = f"""
        DROP TABLE IF EXISTS {table_name};
        
        CREATE TABLE {table_name} (
            {',\n            '.join(columns)}
        );
        
        DROP TRIGGER IF EXISTS update_updated_at ON {table_name};
        
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS 
        $BODY$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $BODY$
        LANGUAGE plpgsql;
        
        CREATE TRIGGER update_updated_at
            BEFORE UPDATE ON {table_name}
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        """
        
        print("\nGenerated SQL:")
        print(sql)
        
        return sql
    
    def prepare_data(self, df: pd.DataFrame, schema_parser: SchemaParser) -> pd.DataFrame:
        field_definitions = schema_parser.get_field_definitions()
        
        for field_name, field_info in field_definitions.items():
            if field_name not in df.columns:
                if field_info.get('required', False):
                    raise ValueError(f"Required field '{field_name}' not found in CSV")
                continue
            
            field_type = schema_parser.get_field_type(field_info)
            field_format = field_info.get('format', '')
            
            try:
                if field_format == 'date':
                    df[field_name] = pd.to_datetime(df[field_name]).dt.date
                elif field_format == 'time':
                    df[field_name] = pd.to_datetime(df[field_name].astype(str).str.strip(), format='%H:%M:%S').dt.time
                elif field_type == 'array':
                    df[field_name] = df[field_name].apply(
                        lambda x: x if isinstance(x, list) else 
                        json.loads(x) if isinstance(x, str) and x.strip().startswith('[') else 
                        [x] if pd.notna(x) else []
                    )
                elif field_type == 'boolean':
                    df[field_name] = df[field_name].map({'true': True, 'false': False})
                elif field_type in ['number', 'integer']:
                    df[field_name] = pd.to_numeric(df[field_name], errors='coerce')
                
                enum_values = schema_parser.get_enum_values(field_info)
                if enum_values:
                    df[field_name] = df[field_name].apply(
                        lambda x: x if x in enum_values else enum_values[0]
                    )
            except Exception as e:
                print(f"Error processing field '{field_name}': {str(e)}")
                raise
        
        return df

    def load_data(self, csv_path: str, schema_path: str) -> None:
        try:
            schema_parser = SchemaParser(schema_path)
            table_name = re.sub(r'[^a-zA-Z0-9_]', '_', Path(csv_path).stem.lower())
            print(f"Using table name: {table_name}")
            
            with self.engine.connect() as conn:
                sql = self.create_table_from_schema(table_name, schema_parser)
                conn.execute(text(sql))
                conn.commit()
                
                verify_sql = """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = :table_name
                ORDER BY ordinal_position;
                """
                result = conn.execute(text(verify_sql), {"table_name": table_name})
                print("\nVerifying table structure:")
                for row in result:
                    print(f"Column: {row[0]}, Type: {row[1]}")
                    
            print(f"Table '{table_name}' created successfully!")
            
            print(f"\nReading CSV file: {csv_path}")
            df = pd.read_csv(csv_path, encoding='utf-8')
            print("\nOriginal columns:")
            print(df.columns.tolist())
            
            print("\nPreparing data...")
            df = self.prepare_data(df, schema_parser)
            print("\nProcessed columns and types:")
            print(df.dtypes)
            
            print("\nLoading data into database...")
            df.to_sql(table_name, self.engine, if_exists='append', index=False, method='multi')
            print("Data loaded successfully!")
            
            with self.engine.connect() as conn:
                row_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                print(f"Total rows loaded: {row_count}")
            
        except Exception as e:
            print(f"\nError occurred: {str(e)}")
            if 'df' in locals():
                print("\nDataframe state at error:")
                print(df.head())
                print("\nData types:")
                print(df.dtypes)

def main():
    parser = argparse.ArgumentParser(description='Generic schema-based data loader')
    parser.add_argument('csv_file', help='Path to the CSV file to load')
    parser.add_argument('schema_file', help='Path to the schema JSON file')
    parser.add_argument('--config', default='config.json', help='Path to database configuration file')
    args = parser.parse_args()
    
    loader = GenericDatabaseLoader(args.config)
    loader.load_data(args.csv_file, args.schema_file)

if __name__ == "__main__":
    main()