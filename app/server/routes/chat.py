"""Chat route - proxies requests to MAS serving endpoint."""
import json
import traceback
from typing import List

import httpx
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from server.config import get_token, get_workspace_host, MAS_ENDPOINT

router = APIRouter()


class Message(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    messages: List[Message]


class ChatResponse(BaseModel):
    response: str


def _extract_text(value) -> str:
    """Recursively extract plain text from nested MAS output structures."""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts = [_extract_text(item) for item in value]
        return '\n'.join(p for p in parts if p)
    if isinstance(value, dict):
        if value.get('type') == 'output_text' and 'text' in value:
            return value['text']
        if 'content' in value:
            return _extract_text(value['content'])
        if 'text' in value:
            return value['text']
    return ''


def _extract_content(data: dict) -> str:
    """Extract text content from MAS response."""
    if 'output' in data:
        text = _extract_text(data['output'])
        return text if text else json.dumps(data['output'])
    if 'choices' in data and data['choices']:
        return data['choices'][0]['message']['content']
    if 'result' in data:
        return data['result']
    return json.dumps(data)


@router.post('/chat', response_model=ChatResponse)
async def chat(req: ChatRequest):
    """Send messages to MAS endpoint and return response."""
    try:
        token = get_token()
        host = get_workspace_host()
        url = f'{host}/serving-endpoints/{MAS_ENDPOINT}/invocations'
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
        }
        payload = {
            'input': [
                {'role': m.role, 'content': m.content} for m in req.messages
            ],
        }
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            return ChatResponse(response=_extract_content(response.json()))

    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        detail = f'MAS endpoint error ({status}): {e.response.text[:500]}'
        raise HTTPException(status_code=502, detail=detail)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post('/chat/stream')
async def chat_stream(req: ChatRequest):
    """Stream responses from MAS endpoint using SSE."""
    try:
        token = get_token()
        host = get_workspace_host()
        url = f'{host}/serving-endpoints/{MAS_ENDPOINT}/invocations'
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
        }
        payload = {
            'input': [
                {'role': m.role, 'content': m.content} for m in req.messages
            ],
            'stream': True,
        }

        async def generate():
            try:
                async with httpx.AsyncClient(timeout=120.0) as client:
                    async with client.stream(
                        'POST', url, headers=headers, json=payload
                    ) as response:
                        if response.status_code != 200:
                            body = await response.aread()
                            error_msg = json.dumps(
                                {'error': f'MAS error: {body.decode()[:500]}'}
                            )
                            yield f'data: {error_msg}\n\n'
                            return
                        async for line in response.aiter_lines():
                            if line.startswith('data: '):
                                yield f'{line}\n\n'
                            elif line.strip():
                                try:
                                    data = json.loads(line)
                                    content = _extract_content(data)
                                    msg = json.dumps({'content': content})
                                    yield f'data: {msg}\n\n'
                                except json.JSONDecodeError:
                                    msg = json.dumps({'content': line})
                                    yield f'data: {msg}\n\n'
                yield 'data: [DONE]\n\n'
            except Exception as e:
                yield f'data: {json.dumps({"error": str(e)})}\n\n'

        return StreamingResponse(generate(), media_type='text/event-stream')

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/health')
async def health():
    """Health check endpoint."""
    return {'status': 'healthy', 'app': 'renew-ai-week'}
