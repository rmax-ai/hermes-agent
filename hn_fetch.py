#!/usr/bin/env python3
"""Fetch top stories from HN Firebase API and filter for AI-related stories."""
import urllib.request
import json
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# AI-related keywords for filtering (case-insensitive)
AI_KEYWORDS = [
    'ai', 'agent', 'llm', 'gpt', 'claude', 'gemini', 'deepseek', 'openai',
    'anthropic', 'mistral', 'llama', 'transformer', 'neural', 'machine learning',
    'rag', 'mcp', 'model context protocol', 'copilot', 'cursor', 'coding assistant',
    'fine-tun', 'embedding', 'token', 'multimodal', 'reasoning', 'chain of thought',
    'inference', 'vllm', 'hugging face', 'langchain', 'langgraph', 'autonomous',
    'foundation model', 'sora', 'veo', 'stable diffusion', 'diffusion', 'gen ai',
    'generative ai', 'codex', 'groq', 'flux', 'ai infra', 'data center', 'chip',
]

def fetch_json(url):
    """Fetch JSON from a URL with retry."""
    for attempt in range(2):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'HN-Digest-Bot/1.0'})
            data = json.loads(urllib.request.urlopen(req, timeout=10).read())
            return data
        except Exception as e:
            if attempt == 1:
                return None
            time.sleep(0.5)

def is_ai_related(title):
    """Check if a story is AI-related based on title."""
    t = title.lower()
    for kw in AI_KEYWORDS:
        if kw in t:
            return True
    return False

def fetch_story(sid):
    """Fetch a single story's details."""
    data = fetch_json(f'https://hacker-news.firebaseio.com/v0/item/{sid}.json')
    if data and data.get('title'):
        return {
            'id': sid,
            'title': data.get('title', ''),
            'url': data.get('url', ''),
            'score': data.get('score', 0),
            'by': data.get('by', ''),
            'descendants': data.get('descendants', 0),
            'time': data.get('time', 0),
        }
    return None

def main():
    # Step 1: Fetch top story IDs
    print("Fetching top story IDs...", file=sys.stderr)
    ids_data = fetch_json('https://hacker-news.firebaseio.com/v0/topstories.json')
    if not ids_data:
        print("[]", end="")
        return
    
    story_ids = ids_data[:100]
    print(f"Got {len(story_ids)} story IDs, fetching concurrently...", file=sys.stderr)
    
    # Fetch all stories concurrently
    all_stories = []
    ai_stories = []
    
    with ThreadPoolExecutor(max_workers=15) as executor:
        fut_map = {executor.submit(fetch_story, sid): sid for sid in story_ids}
        for fut in as_completed(fut_map):
            story = fut.result()
            if story:
                all_stories.append(story)
    
    # Sort by score descending for processing
    all_stories.sort(key=lambda x: x['score'], reverse=True)
    
    for story in all_stories:
        ts = datetime.fromtimestamp(story['time'], tz=timezone.utc)
        story['date_str'] = ts.strftime('%Y-%m-%d %H:%M UTC')
        story['hn_url'] = f'https://news.ycombinator.com/item?id={story["id"]}'
        
        if is_ai_related(story['title']):
            ai_stories.append(story)
            print(f"  AI: {story['title']} (↑{story['score']})", file=sys.stderr)
    
    # Sort AI stories by score descending
    ai_stories.sort(key=lambda x: x['score'], reverse=True)
    
    result = {
        'total_scanned': len(all_stories),
        'ai_count': len(ai_stories),
        'ai_stories': ai_stories,
        'all_top': all_stories[:10],
    }
    
    print(json.dumps(result, indent=2))

if __name__ == '__main__':
    main()
