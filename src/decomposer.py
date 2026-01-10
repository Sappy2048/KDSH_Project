import os
import json
from google import genai
from dotenv import load_dotenv

load_dotenv()

# Configure the Gemini API
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

def get_claims(text: str) -> list[str]:
    """
    Takes a complex backstory and uses Gemini to split it into atomic claims.
    Returns a list of strings.
    """
    if not text or len(text) < 10:
        return []
    
    # Strict prompt to ensure we get a clean JSON list back
    prompt = f"""
    You are a logic engine. Break the following text into atomic, standalone facts (claims).
    Each claim must be a complete sentence containing the entities involved.
    
    TEXT: "{text}"
    
    Return ONLY a raw JSON list of strings. No markdown, no code blocks.
    Example: ["Fact 1", "Fact 2"]
    """
    
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash-lite',
            contents=prompt,
        )
        # Clean up potential markdown formatting (```json ...)
        clean_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(clean_text)
    except Exception as e:
        print(f"Error decomposing text: {e}")
        # Fallback: Just return the original text as a single claim
        return [text]

# Simple test block
if __name__ == "__main__":
    test_text = "Tom Ayrton was a pirate who betrayed Captain Grant."
    print(get_claims(test_text))
