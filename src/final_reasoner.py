import os
from google import genai
from dotenv import load_dotenv

load_dotenv()
client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

def generate_verdict(backstory_claim, text_evidence, graph_evidence):
    """
    Compares a single claim against search results to determine if it's true.
    """
    
    prompt = f"""
    You are a Fact-Checker. Compare the 'Claim' against the 'Evidence' provided.
    
    CLAIM: "{backstory_claim}"
    
    TEXT EVIDENCE: {text_evidence}
    
    GRAPH EVIDENCE: {graph_evidence}
    
    TASK:
    1. VERDICT: Is the claim supported by the evidence? (Supported / Contradicted / Not Enough Info).
    2. RATIONALE: Briefly explain why based ONLY on the provided evidence.
    
    Return the result in this exact format:
    VERDICT: [Your Verdict]
    RATIONALE: [Your Rationale]
    """
    
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash-lite',
            contents=prompt,
        )
        text = response.text.strip()
        # Split into Verdict and Rationale
        parts = text.split("RATIONALE:")
        verdict = parts[0].replace("VERDICT:", "").strip()
        rationale = parts[1].strip() if len(parts) > 1 else "No rationale provided."
        
        return verdict, rationale
    except Exception as e:
        return "Error", f"Failed to reason: {e}"
