import yaml
from pydantic import BaseModel

class ModelConfig(BaseModel):
    provider: str
    name: str
    temperature: float

class ProcessingConfig(BaseModel):
    chunk_size: int
    chunk_overlap: int

class EmbeddingConfig(BaseModel):
    model: str

class StorageConfig(BaseModel):
    json_store: str
    index_path: str
    persist: bool

class AppConfig(BaseModel):
    model: ModelConfig
    processing: ProcessingConfig
    embedding: EmbeddingConfig
    storage: StorageConfig

def load_config(config_path: str = '../src/config.yaml') -> AppConfig:
    """
    Load application configuration from a YAML file.
    """
    with open(config_path, 'r') as f:
        cfg = yaml.safe_load(f)
    return AppConfig(**cfg)