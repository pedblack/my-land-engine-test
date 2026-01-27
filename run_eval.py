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

# Model Options
MODELS = {
    "flash": "gemini-2.0-flash-001",
    "lite": "gemini-2.0-flash-lite-preview-02-05"
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

def extract_json_content(text):
    """Robust extraction removing markdown code blocks."""
    text = re.sub(r"```json\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"```\s*", "", text)
    return text.strip()

async def process_batch(client, model_name, system_instruction, batch_reviews, start_index):
    """Sends a single batch to the LLM and handles wrapped responses."""
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
        
        raw_text = extract_json_content(response.text)
        
        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError:
            print(f"   ⚠️ JSON Decode Error in batch {start_index}. Response preview: {raw_text[:100]}...")
            return []

        # --- SMART UNWRAPPER ---
        if isinstance(parsed, list):
            return parsed
        
        if isinstance(parsed, dict):
            # Check common keys
            for key in ["reviews", "data", "results", "output", "items", "analysis"]:
                if key in parsed and isinstance(parsed[key], list):
                    return parsed[key]
            # If strictly one key exists and it's a list
            if len(parsed) == 1:
                key = list(parsed.keys())[0]
                if isinstance(parsed[key], list):
                    return parsed[key]

            print(f"   ⚠️ Parsed JSON is a dict but couldn't find the list. Keys: {list(parsed.keys())}")
            return []

        return []

    except Exception as e:
        print(f"   ⚠️ API/Network Error in batch {start_index}: {str(e)}")
        return []

async def run_evaluation(model_key: str, limit: int, batch_size: int):
    client = genai.Client(api_key=API_KEY)
    model_name = MODELS.get(model_key)
    if not model_name:
        raise ValueError(f"Unknown model key: {model_key}")

    print(f"🚀 Loading Data...")
    gold_data = load_data(limit)
    total_items = len(gold_data)
    print(f"   Loaded {total_items} items to evaluate.")

    print(f"📜 Loading Prompt...")
    system_instruction = load_prompt()

    predictions = []
    
    # Determine effective batch size
    # If batch_size is 0, we process all items in one go.
    effective_batch_size = total_items if batch_size <= 0 else batch_size
    
    print(f"🤖 Sending requests to {model_name}...")
    if batch_size <= 0:
        print(f"   Mode: SINGLE CALL (Batch size: {total_items})")
    else:
        print(f"   Mode: BATCHED (Batch size: {effective_batch_size})")

    # --- PROCESSING LOOP ---
    for i in range(0, total_items, effective_batch_size):
        batch_gold = gold_data[i : i + effective_batch_size]
        batch_reviews = [item["review"] for item in batch_gold]
        
        current_batch_num = (i // effective_batch_size) + 1
        print(f"   Processing batch {current_batch_num} (Items {i} to {i+len(batch_reviews)})...")
        
        batch_preds = await process_batch(client, model_name, system_instruction, batch_reviews, i)
        
        if batch_preds:
            predictions.extend(batch_preds)
        else:
            print(f"   ❌ Batch {current_batch_num} failed or returned empty.")

    # --- SCORING ---
    print("\n📊 Calculating Metrics...")
    
    total_tp, total_fp, total_fn = 0, 0, 0
    
    for i, gold_item in enumerate(gold_data):
        gold_pros = set(gold_item.get("pros", []))
        gold_cons = set(gold_item.get("cons", []))
        
        if i < len(predictions):
            pred_item = predictions[i]
            pred_pros = set(pred_item.get("pros", []))
            pred_cons = set(pred_item.get("cons", []))
        else:
            pred_pros, pred_cons = set(), set()

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
    print(f"Samples Evaluated: {total_items}")
    print(f"Batch Size Used:   {effective_batch_size}")
    print("-" * 40)
    print(f"Precision: {precision:.2%}")
    print(f"Recall:    {recall:.2%}")
    print(f"F1 Score:  {f1:.2%}")
    print("-" * 40)
    print(f"Raw Counts -> TP: {total_tp}, FP: {total_fp}, FN: {total_fn}")
    print("="*40)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10, help="Number of reviews to evaluate (0 for all)")
    parser.add_argument("--batch_size", type=int, default=10, help="Items per API call (0 for single call)")
    parser.add_argument("--model", type=str, choices=["flash", "lite"], default="lite")
    args = parser.parse_args()
    
    asyncio.run(run_evaluation(args.model, args.limit, args.batch_size))
