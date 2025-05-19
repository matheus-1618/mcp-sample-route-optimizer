import boto3
import json
import os
import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport
from pydantic import Field
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route
from typing import Dict, Union, Any, Optional
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware import Middleware

# Get base path from environment variable or default to empty string
BASE_PATH = os.environ.get('BASE_PATH', '')

# Initialize FastMCP server
mcp = FastMCP(
    name='dynamodb-server',
    instructions='The official MCP Server for interacting with AWS DynamoDB',
    version='0.1.1',
)

class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        # Skip authentication for health check endpoint
        if request.url.path.endswith('/'):
            return await call_next(request)
            
        # Get the expected token from environment variable
        expected_token = os.environ.get('API_TOKEN',"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkxvY2F0aW9uU2VydmljZUNsaWVudCIsImlhdCI6MTY5ODc2NTQzOX0")
        
        # If no token is configured, skip authentication
        if not expected_token:
            return await call_next(request)
            
        # Check for Authorization header
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return JSONResponse(
                {"error": "Authorization header missing"}, 
                status_code=401
            )
            
        # Validate the token format
        parts = auth_header.split()
        if len(parts) != 2 or parts[0].lower() != 'bearer':
            return JSONResponse(
                {"error": "Invalid authorization format. Use 'Bearer {token}'"}, 
                status_code=401
            )
            
        # Validate the token value
        token = parts[1]
        if token != expected_token:
            return JSONResponse(
                {"error": "Invalid token"}, 
                status_code=401
            )
            
        # Token is valid, proceed with the request
        return await call_next(request)
    
# Create SSE transport
sse = SseServerTransport(f'{BASE_PATH}/messages/')


def get_dynamodb_client():
    """Create a boto3 DynamoDB client using credentials from environment variables."""
    # Always use us-east-1 region
    region = 'us-east-1'

    # Create a new session to force credentials to reload
    session = boto3.Session()

    # boto3 will automatically load credentials from environment variables
    return session.client('dynamodb', region_name=region)

def parse_input(input_value, default=None):
    """Parse input that could be a JSON string or a dictionary"""
    if input_value is None:
        return default
    
    # If it's already a dict, return it
    if isinstance(input_value, dict):
        return input_value
    
    # If it's a string, try to parse it as JSON
    if isinstance(input_value, str):
        try:
            return json.loads(input_value)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format: {str(e)}. Please provide valid JSON.")
    
    # If it's neither a dict nor a string, raise an error
    raise ValueError(f"Expected a JSON string or a dictionary, got {type(input_value).__name__}")

def format_dynamodb_key(key_value, key_type=None):
    """Convert a simple value to DynamoDB format based on type"""
    if key_type is None:
        # Try to infer type
        if isinstance(key_value, int):
            key_type = 'N'
        elif isinstance(key_value, bool):
            key_type = 'BOOL'
        else:
            key_type = 'S'  # Default to string
    
    if key_type == 'N':
        return {'N': str(key_value)}
    elif key_type == 'BOOL':
        return {'BOOL': key_value}
    else:
        return {'S': str(key_value)}

def format_expression_attribute_values(values_dict):
    """Convert a simple dict to DynamoDB ExpressionAttributeValues format"""
    result = {}
    for key, value in values_dict.items():
        if not key.startswith(':'):
            key = f":{key}"
        
        if isinstance(value, int) or isinstance(value, float):
            result[key] = {'N': str(value)}
        elif isinstance(value, bool):
            result[key] = {'BOOL': value}
        else:
            result[key] = {'S': str(value)}
    
    return result

# MCP SSE handler function
async def handle_sse(request):
    async with sse.connect_sse(request.scope, request.receive, request._send) as (
        read_stream,
        write_stream,
    ):
        await mcp._mcp_server.run(
            read_stream, write_stream, mcp._mcp_server.create_initialization_options()
        )

@mcp.tool()
async def update_item(
    table_name: str = Field(description='Table Name'),
    partition_key: str = Field(description='Partition key value'),
    partition_key_name: str = Field(description='Name of the partition key attribute'),
    update_expression: str = Field(
        description='Defines the attributes to be updated. Example: "SET name = :name"'
    ),
    expression_values: str = Field(
        description='JSON string of values to use in the update. Example: {"name": "John", "age": 30}'
    )
) -> dict:
    """Updates an item in DynamoDB using simplified parameters."""
    client = get_dynamodb_client()
    
    try:
        # Format the key in DynamoDB format
        key = {
            partition_key_name: format_dynamodb_key(partition_key)
        }
        
        # Parse and format expression attribute values
        values_dict = json.loads(expression_values)
        expression_attribute_values = format_expression_attribute_values(values_dict)
        
        params = {
            'TableName': table_name,
            'Key': key,
            'UpdateExpression': update_expression,
            'ExpressionAttributeValues': expression_attribute_values,
            'ReturnConsumedCapacity': 'TOTAL'
        }

        response = client.update_item(**params)
        return {
            'Attributes': response.get('Attributes'),
            'ConsumedCapacity': response.get('ConsumedCapacity'),
        }
    except Exception as e:
        return {'Error': str(e)}



@mcp.tool()
async def put_item(
    table_name: str = Field(description='Table Name'),
    item_data: str = Field(
        description='Item data as a simple JSON string. Example: {"id": "123", "name": "test", "age": 30}'
    )
) -> dict:
    """Creates or replaces an item in DynamoDB using simplified item format."""
    client = get_dynamodb_client()
    
    try:
        # Parse the simple JSON into a dict
        simple_item = json.loads(item_data)
        
        # Convert to DynamoDB format
        dynamodb_item = {}
        for key, value in simple_item.items():
            if isinstance(value, int) or isinstance(value, float):
                dynamodb_item[key] = {'N': str(value)}
            elif isinstance(value, bool):
                dynamodb_item[key] = {'BOOL': value}
            else:
                dynamodb_item[key] = {'S': str(value)}
        
        params = {
            'TableName': table_name,
            'Item': dynamodb_item,
            'ReturnConsumedCapacity': 'TOTAL'
        }

        response = client.put_item(**params)
        return {
            'Attributes': response.get('Attributes'),
            'ConsumedCapacity': response.get('ConsumedCapacity'),
        }
    except Exception as e:
        return {'Error': str(e)}



@mcp.tool()
async def scan(
    table_name: str = Field(description='Table Name or Amazon Resource Name (ARN)')
) -> dict:
    """Returns items and attributes by scanning a table or secondary index."""
    client = get_dynamodb_client()
    
    try:
        params = {'TableName': table_name}
        params['ReturnConsumedCapacity'] = 'TOTAL'

        response = client.scan(**params)
        return {
            'Items': response.get('Items', []),
            'Count': response.get('Count'),
            'ScannedCount': response.get('ScannedCount'),
            'LastEvaluatedKey': response.get('LastEvaluatedKey'),
            'ConsumedCapacity': response.get('ConsumedCapacity'),
        }
    except Exception as e:
        return {'Error': str(e)}

@mcp.tool()
async def list_tables(
    limit: Optional[int] = Field(
        default=None,
        description='Max number of table names to return',
    ),
) -> dict:
    """Returns a paginated list of table names in your account."""
    client = get_dynamodb_client()
    
    try:
        params = {}
        if limit:
            params['Limit'] = limit
        
        response = client.list_tables(**params)
        return {
            'TableNames': response['TableNames'],
            'LastEvaluatedTableName': response.get('LastEvaluatedTableName'),
        }
    except Exception as e:
        return {'Error': str(e)}

@mcp.tool()
async def get_table_schema(
    table_name: str = Field(description='Table Name or Amazon Resource Name (ARN)'),
) -> dict:
    """Returns table information including key schema and indexes."""
    client = get_dynamodb_client()
    
    try:
        response = client.describe_table(TableName=table_name)
        table_info = response['Table']
        
        # Extract just the schema information
        schema = {
            'TableName': table_info['TableName'],
            'KeySchema': table_info.get('KeySchema', []),
            'AttributeDefinitions': table_info.get('AttributeDefinitions', []),
            'GlobalSecondaryIndexes': table_info.get('GlobalSecondaryIndexes', []),
            'LocalSecondaryIndexes': table_info.get('LocalSecondaryIndexes', []),
        }
        
        return schema
    except Exception as e:
        return {'Error': str(e)}

# Add a health check route handler
async def health_check(request):
    return JSONResponse({'status': 'healthy', 'service': 'dynamodb-server'})

if __name__ == '__main__':
    # Set port to 6000
    port = 5500

    # Create our custom app with both the health check and the SSE endpoints
    app = Starlette(
        routes=[
            Route(f'{BASE_PATH}/', health_check),
            Route(f'{BASE_PATH}/sse', endpoint=handle_sse),
            Mount(f'{BASE_PATH}/messages/', app=sse.handle_post_message),
        ],
        middleware=[
            Middleware(AuthMiddleware)
        ]
    )

    print(f'Starting MCP DynamoDB server on port {port}. Press CTRL+C to exit.')
    
    # For container environments like Fargate/ECS, we need to bind to 0.0.0.0
    uvicorn.run(
        app,
        host='0.0.0.0',  # nosec B104
        port=port,
        timeout_graceful_shutdown=2,  # Only wait 2 seconds for connections to close
    )