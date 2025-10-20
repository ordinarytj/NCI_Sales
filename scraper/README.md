ESB Sales Scraper

This is a small configurable Python scraper scaffold to collect company sales data from the ESB website.

Quick start

1. Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Edit `config_example.yaml` to point to the correct `start_urls`, `list_selector`, and `fields` selectors for the ESB site.

3. Run the scraper:

```bash
python scrape_esb.py --config config_example.yaml
```

Notes
- This scaffold uses requests + BeautifulSoup. If the ESB site needs JavaScript rendering, we'll add a Playwright or Selenium implementation.
- The scraper respects a minimal robots.txt check. For production usage, improve robots handling and add politeness features (backoff, proxy, authentication).

Next steps
- Provide the ESB URL(s) and whether login is required.
- If login required, provide credentials securely or an API token. I'll add an authenticated session and adapt selectors.
