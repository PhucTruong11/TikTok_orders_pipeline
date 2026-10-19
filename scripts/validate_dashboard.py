"""Exercise dashboard filters against the validated two-channel fixture."""
import json
import os
from pathlib import Path

from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[1]


def validate():
    summary = json.loads((ROOT / ".validation/latest_validation.json").read_text(encoding="utf-8"))
    database = Path(summary["fixture_database"])
    previous = os.environ.get("DBT_DUCKDB_PATH")
    try:
        os.environ["DBT_DUCKDB_PATH"] = str(database)
        app = AppTest.from_file(str(ROOT / "dashboards/streamlit_app/app.py"), default_timeout=30).run()
        assert not app.exception, [e.message for e in app.exception]
        # Default intersection is Jan 1: 5 TikTok + 6 Shopee VND records.
        assert app.metric[0].value == "11", app.metric[0].value
        assert app.metric[1].value == "2"
        app.checkbox[0].set_value(False).run()
        # Whole fixture period has 7 TikTok + 6 Shopee VND records.
        assert not app.exception
        assert app.metric[0].value == "13"
        app.selectbox[0].set_value("SGD").run()
        assert not app.exception
        assert app.metric[0].value == "2"
        app.multiselect[0].set_value(["shopee"]).run()
        assert not app.exception
        assert app.metric[0].value == "1"
        app.selectbox[0].set_value("VND").run()
        assert not app.exception
        assert app.metric[0].value == "6"
        app.multiselect[0].set_value([]).run()
        assert not app.exception
        assert any("Chọn ít nhất" in message.value for message in app.info)
        # Missing database is a helpful empty state, not an exception.
        os.environ["DBT_DUCKDB_PATH"] = str(database.parent / "not_created.duckdb")
        empty = AppTest.from_file(str(ROOT / "dashboards/streamlit_app/app.py"), default_timeout=30).run()
        assert not empty.exception
        assert any("Chưa có warehouse" in message.value for message in empty.info)
    finally:
        if previous is None:
            os.environ.pop("DBT_DUCKDB_PATH", None)
        else:
            os.environ["DBT_DUCKDB_PATH"] = previous
    print("Dashboard passed: two sources, currency/source filters, empty selection and missing-database state")


if __name__ == "__main__":
    validate()
