import os
import pandas as pd
from typing import List, Dict
from langchain_ollama import OllamaLLM
from src.utils.config import load_config
from src.prompts.enrichment import (
    get_enrichment_system_prompt,
    get_enrichment_user_prompt
)

class EnrichmentPipeline:
    """
    Enrich user profiles by batching customer metrics and calling an LLM.
    """
    def __init__(self, batch_size: int = None):
        # Load configuration
        cfg = load_config()
        self.batch_size = batch_size or cfg.processing.batch_size
        self.model_name = cfg.model.name
        self.temperature = cfg.model.temperature
        # Initialize LLM client
        self.llm = OllamaLLM(model=self.model_name, temperature=self.temperature, keep_alive="15m")
        # Setup enrichment persistence
        project_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', '..')
        )
        self.enriched_dir = os.path.join(project_root, 'data', 'enriched')
        os.makedirs(self.enriched_dir, exist_ok=True)
        self.enriched_path = os.path.join(self.enriched_dir, 'enriched_profiles.parquet')

    def enrich(
        self,
        scored_df: pd.DataFrame,
        feature_meta_df: pd.DataFrame
    ) -> pd.DataFrame:
        # Load existing enriched profiles if present
        if os.path.exists(self.enriched_path):
            existing_df = pd.read_parquet(self.enriched_path)
        else:
            existing_df = pd.DataFrame(columns=['customer_id', 'profile'])
        # Determine which customers need enrichment
        scored_df['customer_id'] = scored_df['customer_id'].astype(str)
        enriched_ids = set(existing_df['customer_id'].astype(str))
        to_enrich = scored_df[~scored_df['customer_id'].isin(enriched_ids)]
        # If no new customers, return existing
        if to_enrich.empty:
            return existing_df
        # Prepare metadata list
        feature_meta_list: List[Dict] = feature_meta_df.to_dict(orient='records')
        new_results: List[Dict] = []
        records = to_enrich.to_dict(orient='records')
        # Batch through LLM
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            system_prompt = get_enrichment_system_prompt()
            user_prompt = get_enrichment_user_prompt(batch, feature_meta_list)
            full_prompt = system_prompt + '\n' + user_prompt
            print(full_prompt)
            response = self.llm.invoke(full_prompt)
            print(response)
            # Parse LLM lines
            for ln in response.split('\n'):
                ln = ln.strip()
                if not ln or not ln.lower().startswith('customer_id:'):
                    continue
                try:
                    cust_part, prof_part = ln.split('profile:', 1)
                    cid = cust_part.split(':', 1)[1].strip().strip(',')
                    profile_text = prof_part.strip()
                    new_results.append({'customer_id': cid, 'profile': profile_text})
                except Exception:
                    print(Exception)
                    continue
        # Combine and persist
        new_df = pd.DataFrame(new_results)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        combined.to_parquet(self.enriched_path, index=False)
        return combined