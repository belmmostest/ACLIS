from typing import List, Dict

def get_enrichment_system_prompt() -> str:
    return (
        "You are a customer-lifecycle intelligence assistant. "
        "Generate concise narrative profiles for each customer "
        "based on their metrics and feature definitions. "
        "Focus on key behaviors, trends, and actionable insights present in the customer metrics and features."
    )

def get_enrichment_user_prompt(
    metrics_batch: List[Dict],
    feature_metadata: List[Dict],
    max_profile_words: int = 150
) -> str:
    prompt = "Feature Definitions:\n"
    for fm in feature_metadata:
        col = fm.get('Column') or fm.get('column')
        desc = fm.get('Description') or fm.get('description', '')
        prompt += f"- {col}: {desc}\n"
    prompt += "\nCustomer Metrics:\n"
    for m in metrics_batch:
        cid = m.get('customer_id') or m.get('customer_id')
        metrics_str = ', '.join(f"{k}={v}" for k, v in m.items() if k != 'customer_id')
        prompt += f"- customer_id: {cid}, {metrics_str}\n"
    prompt += "\nInstructions:\n"
    prompt += (
        "For each customer listed above, output a single line in the following format exactly:\n"
        "customer_id: <ID>, profile: <concise summary>\n"
    )
    prompt += f"Make sure all customer_ids are covered and the profiles provide valuable insights.\n"
    return prompt