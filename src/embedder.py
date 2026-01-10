import os
from google import genai
from google.genai import types
import pathway as pw
import numpy as np

class GeminiEmbedder:
    def __init__(self, model="models/embedding-001"):
        self.client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
        self.model = model

    def __call__(self, text: str) -> np.ndarray:
        """
        Calculates the embedding for a single string.
        Returns a numpy array (vector).
        """
        if not text or not text.strip():
            # Return a zero vector or handle empty text gracefully
            return np.zeros(768) 
            
        try:
            response = self.client.models.embed_content(
                model=self.model,
                contents=text,
                config=types.EmbedContentConfig(
                    output_dimensionality=768  # Standard size for this model
                )
            )
            return np.array(response.embeddings[0].values)
        except Exception as e:
            print(f"Embedding error: {e}")
            return np.zeros(768)

# Wrap it as a Pathway UDF so it can be distributed
@pw.udf
def get_embedding(text: str) -> np.ndarray:
    embedder = GeminiEmbedder()
    return embedder(text)
