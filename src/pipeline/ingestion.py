import os
import glob
import pandas as pd
from datetime import datetime
from typing import Dict, Any

class IngestionPipeline:
    """
    Ingest raw CSV data and stage it as Parquet files with versioning.
    """
    def __init__(self,
                 data_dir: str = None,
                 staging_dir: str = None):
        project_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', '..')
        )
        self.data_dir = data_dir or os.path.join(project_root, 'data')
        self.staging_dir = staging_dir or os.path.join(self.data_dir, 'staging')
        os.makedirs(self.staging_dir, exist_ok=True)
        self.timestamp = datetime.now().strftime('%Y%m%dT%H%M%S')
        self.manifest_path = os.path.join(self.staging_dir, 'manifest.json')
        self.manifest = self._load_manifest()

    def load_scored_data(self) -> pd.DataFrame:
        path = os.path.join(self.data_dir, 'scored_data.csv')
        return pd.read_csv(path)

    def load_feature_metadata(self) -> pd.DataFrame:
        path = os.path.join(self.data_dir, 'column_and_feature_metadata.csv')
        return pd.read_csv(path)

    def load_offers(self) -> (Dict[str, pd.DataFrame], pd.DataFrame):
        offers_dir = os.path.join(self.data_dir, 'offers')
        files = glob.glob(os.path.join(offers_dir, '*.csv'))
        offers: Dict[str, pd.DataFrame] = {}
        offers_meta: pd.DataFrame = None
        for fp in files:
            fn = os.path.basename(fp)
            if fn == 'offers_metadata.csv':
                offers_meta = pd.read_csv(fp)
            else:
                key = os.path.splitext(fn)[0]
                offers[key] = pd.read_csv(fp)
        return offers, offers_meta
    def _load_manifest(self) -> Dict[str, Any]:
        if os.path.exists(self.manifest_path):
            try:
                import json
                with open(self.manifest_path, 'r') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_manifest(self):
        import json
        with open(self.manifest_path, 'w') as f:
            json.dump(self.manifest, f, indent=2)

    def _compute_hash(self, file_path: str) -> str:
        import hashlib
        hash_md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def stage_file(self, src_path: str, df: pd.DataFrame):
        rel_path = os.path.relpath(src_path, self.data_dir)
        file_hash = self._compute_hash(src_path)
        entry = self.manifest.get(rel_path)
        if entry and entry.get('hash') == file_hash:
            return entry.get('staged_path')
        base = os.path.splitext(rel_path.replace(os.sep, '_'))[0]
        dest_name = f"{base}_{self.timestamp}.parquet"
        dest_path = os.path.join(self.staging_dir, dest_name)
        df.to_parquet(dest_path, index=False)
        self.manifest[rel_path] = {'hash': file_hash, 'staged_path': dest_name}
        self._save_manifest()
        return dest_name

    def stage_scored_data(self, df: pd.DataFrame):
        src = os.path.join(self.data_dir, 'scored_data.csv')
        self.stage_file(src, df)

    def stage_feature_metadata(self, df: pd.DataFrame):
        src = os.path.join(self.data_dir, 'column_and_feature_metadata.csv')
        self.stage_file(src, df)

    def stage_offers(self,
                     offers: Dict[str, pd.DataFrame],
                     offers_meta: pd.DataFrame):
        # Stage offers metadata
        meta_src = os.path.join(self.data_dir, 'offers', 'offers_metadata.csv')
        self.stage_file(meta_src, offers_meta)
        # Stage individual offers
        for key, df in offers.items():
            src = os.path.join(self.data_dir, 'offers', f"{key}.csv")
            self.stage_file(src, df)

    def run(self) -> Dict[str, Any]:
        scored_df = self.load_scored_data()
        feature_meta_df = self.load_feature_metadata()
        offers_dict, offers_meta_df = self.load_offers()
        # Stage only new or changed files
        self.stage_scored_data(scored_df)
        self.stage_feature_metadata(feature_meta_df)
        self.stage_offers(offers_dict, offers_meta_df)
        return {
            'scored_data': scored_df,
            'feature_metadata': feature_meta_df,
            'offers': offers_dict,
            'offers_metadata': offers_meta_df
        }
        return {
            'scored_data': scored_df,
            'feature_metadata': feature_meta_df,
            'offers': offers_dict,
            'offers_metadata': offers_meta_df
        }