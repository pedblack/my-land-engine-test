import argparse
import json
import os
import asyncio
import re
from typing import List, Dict, Set
from google import genai
from google.genai import types

# --- CONFIGURATION ---
API_KEY = os.environ.get("GOOGLE_API_KEY")
EVAL_SET_FILE = "eval_set.json"
PROMPT_FILE = "llm_prompt.txt"
BATCH_SIZE = 10  # Process 10 reviews per call to avoid token limits

# Model Options
MODELS = {
    "flash": "gemini-2.5-flash",
    "lite": "gemini-2.5-flash-lite"
}

def load_data(limit: int = 0):
    if not os.path.exists(EVAL_SET_FILE):
        raise FileNotFoundError(f"❌ Could not find {EVAL_SET_FILE}")
    
    with open(EVAL_SET_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    if limit > 0:
        return data[:limit]
    return data

def load_prompt():
    if not os.path.exists(PROMPT_FILE):
        raise FileNotFoundError(f"❌ Could not find {PROMPT_FILE}")
    with open(PROMPT_FILE, "r", encoding="utf-8") as f:
        return f.read()

def calculate_metrics(gold_set: Set[str], pred_set: Set[str]):
    tp = len(gold_set.intersection(pred_set))
    fp = len(pred_set - gold_set)
    fn = len(gold_set - pred_set)
    return tp, fp, fn

def clean_json_text(text):
    """Removes markdown backticks if present."""
    # Remove ```json and ``` or just ```
    if text.startswith("```"):
        text = re.sub(r"^```json\s*", "", text)
        text = re.sub(r"^```\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()

async def process_batch(client, model_name, system_instruction, batch_reviews, start_index):
    """Sends a single batch to the LLM."""
    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        temperature=0.0,
        system_instruction=system_instruction,
    )

    try:
        response = await client.aio.models.generate_content(
            model=model_name,
            contents=json.dumps(batch_reviews),
            config=config,
        )
        
        clean_text = clean_json_text(response.text)
        return json.loads(clean_text)
    except Exception as e:
        print(f"⚠️ Error in batch starting at {start_index}: {str(e)[:100]}...")
        return []

async def run_evaluation(model_key: str, limit: int):
    client = genai.Client(api_key=API_KEY)
    model_name = MODELS.get(model_key)
    
    if not model_name:
        raise ValueError(f"Unknown model key: {model_key}")

    print(f"🚀 Loading Data...")
    gold_data = load_data(limit)
    print(f"   Loaded {len(gold_data)} items to evaluate.")

    print(f"📜 Loading Prompt...")
    system_instruction = load_prompt()

    predictions = []
    
    # --- BATCH PROCESSING LOOP ---
    total_items = len(gold_data)
    print(f"🤖 Sending requests to {model_name} in batches of {BATCH_SIZE}...")
    
    for i in range(0, total_items, BATCH_SIZE):
        batch_gold = gold_data[i : i + BATCH_SIZE]
        batch_reviews = [item["review"] for item in batch_gold]
        
        print(f"   Processing batch {i//BATCH_SIZE + 1} ({len(batch_reviews)} items)...")
        
        batch_preds = await process_batch(client, model_name, system_instruction, batch_reviews, i)
        
        # Validation: Ensure we got a list back
        if isinstance(batch_preds, list):
            predictions.extend(batch_preds)
        else:
            print(f"   ❌ Batch {i//BATCH_SIZE + 1} returned invalid format (not a list).")

    # --- SCORING ---
    print("\n📊 Calculating Metrics...")
    
    total_tp, total_fp, total_fn = 0, 0, 0
    
    # Map predictions by index to handle potential mismatches safely
    # We assume sequential processing, but safety first.
    # Since we extended `predictions` in order, index j in predictions maps to index j in gold_data
    
    for i, gold_item in enumerate(gold_data):
        gold_pros = set(gold_item.get("pros", []))
        gold_cons = set(gold_item.get("cons", []))
        
        if i < len(predictions):
            pred_item = predictions[i]
            # Handle case where LLM returns object with 'pros'/'cons' keys
            pred_pros = set(pred_item.get("pros", []))
            pred_cons = set(pred_item.get("cons", []))
        else:
            # If the LLM crashed or returned fewer items, count as misses (FN)
            pred_pros, pred_cons = set(), set()

        # Combine Pros & Cons for a single F1 score
        # (You can split this if you want separate metrics)
        gold_all = gold_pros.union(gold_cons)
        pred_all = pred_pros.union(pred_cons)

        tp, fp, fn = calculate_metrics(gold_all, pred_all)
        total_tp += tp
        total_fp += fp
        total_fn += fn

    precision = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0
    recall = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

    print("\n" + "="*40)
    print(f"EVAL REPORT: {model_name}")
    print("="*40)
    print(f"Samples Evaluated: {len(gold_data)}")
    print(f"Precision: {precision:.2%}")
    print(f"Recall:    {recall:.2%}")
    print(f"F1 Score:  {f1:.2%}")
    print("-" * 40)
    print(f"Raw Counts -> TP: {total_tp}, FP: {total_fp}, FN: {total_fn}")
    print("="*40)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10, help="Number of reviews (0 for all)")
    parser.add_argument("--model", type=str, choices=["flash", "lite"], default="lite")
    args = parser.parse_args()
    
    asyncio.run(run_evaluation(args.model, args.limit))
