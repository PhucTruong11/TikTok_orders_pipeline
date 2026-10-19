"""Build both sources, validate the candidate and atomically publish the local DB."""
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline_orchestration.warehouse import build_and_publish

if __name__ == "__main__":
    print(json.dumps(build_and_publish(), indent=2))
