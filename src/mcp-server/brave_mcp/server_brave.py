import httpx
import os
import uvicorn
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware import Middleware


from brave_search_python_client import (
    BraveSearch,
    WebSearchApiResponse,
    WebSearchRequest,
)

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route
from typing import Any

# Get base path from environment variable or default to empty string
BASE_PATH = os.environ.get('BASE_PATH', '')

# Load environment variables from .env file
load_dotenv()

# Initialize FastMCP server
mcp = FastMCP('brave-search')

# Create SSE transport
sse = SseServerTransport(f'{BASE_PATH}/messages/')

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
async def search_web(query: str) -> str:
    """Search the web using Brave Search.

    Args:
        query: The search query
    """
    api_key = os.environ.get('BRAVE_SEARCH_API_KEY')
    if not api_key:
        return "Error: Brave Search API key not found in environment variables."
    
    try:
        # Initialize the BraveSearch client
        bs = BraveSearch(api_key=api_key)
        
        # Perform the search
        response = await bs.web(WebSearchRequest(q=query))
        
        # Format the results
        results = []
        if response.web and response.web.results:
            for i, result in enumerate(response.web.results[:5]):  # Limit to top 5 results
                results.append(f"{i+1}. {result.title}\n   URL: {result.url}\n   {result.description}\n")
            
            return "\n".join(results)
        else:
            return "No results found for your query."
    except Exception as e:
        return f"Error performing search: {str(e)}"

@mcp.tool()
async def inspect_url(url: str) -> str:
    """Retrieve and extract the content from a given URL.
    
    Args:
        url: The URL to inspect
    """
    try:
        # Create request with a browser-like user agent to avoid being blocked
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        
        # Open the URL and read the content
        with urlopen(req, timeout=10) as response:
            if response:
                # Parse the HTML content
                soup = BeautifulSoup(response.read().decode('utf-8', errors='replace'), 'html.parser')
                
                # Remove script and style elements
                for script_or_style in soup(["script", "style"]):
                    script_or_style.decompose()
                
                # Extract and clean the text
                text = soup.get_text()
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                cleaned_text = '\n'.join(chunk for chunk in chunks if chunk)
                
                return cleaned_text
            else:
                return "Error: No response from the server."
    except Exception as e:
        return f"Error while inspecting URL {url}: {str(e)}"

# Add a health check route handler
async def health_check(request):
    return JSONResponse({'status': 'healthy', 'service': 'brave-search-api-python'})

if __name__ == '__main__':
    # For simplicity, let's use the built-in run method
    # This automatically sets up both the SSE and message endpoints
    port = int(os.environ.get('PORT', 5500))

    sse_app = mcp.sse_app()

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

    print(f'Starting MCP Brave Search server on port {port}. Press CTRL+C to exit.')
    
    # For container environments like Fargate/ECS, we need to bind to 0.0.0.0
    # This is accepted as a necessary risk for containerized deployments
    # nosec B104 - Binding to all interfaces is required for container environments
    uvicorn.run(
        app,
        host='0.0.0.0',  # nosec B104
        port=port,
        timeout_graceful_shutdown=2,  # Only wait 2 seconds for connections to close
    )