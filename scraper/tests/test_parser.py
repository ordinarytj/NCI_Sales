import sys
from pathlib import Path

# Ensure project root is on sys.path so tests can import the module
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

from scrape_esb import parse_list_page


def test_parse_list_page_basic():
    html = """
    <html><body>
    <div class="company-row">
      <span class="company-name">ABC Corp</span>
      <span class="period">2024-Q1</span>
      <span class="sales">1,000,000</span>
    </div>
    <div class="company-row">
      <span class="company-name">XYZ Ltd</span>
      <span class="period">2024-Q1</span>
      <span class="sales">500,000</span>
    </div>
    </body></html>
    """
    config = {
        "list_selector": ".company-row",
        "fields": {
            "company_name": {"selector": ".company-name", "type": "text"},
            "period": {"selector": ".period", "type": "text"},
            "sales": {"selector": ".sales", "type": "text"},
        },
    }
    rows = parse_list_page(html, config)
    assert len(rows) == 2
    assert rows[0]["company_name"] == "ABC Corp"
    assert rows[1]["sales"] == "500,000"
