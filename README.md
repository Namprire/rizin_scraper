# X Free-plan Scraper (RIZIN vs UFC)

Commands:
  python -m x_free_scraper status
  python -m x_free_scraper scout --query-key spectacle_en
  python -m x_free_scraper fetch --query-key spectacle_en --max-results 10 --anonymize
  python -m x_free_scraper reset --what monthly

Notes:
- Free plan hard guards: ≤100 posts/month, and 1 request per 15 minutes for counts & recent search.
- This tool exits with code 2 on time guard, 3 on quota guard, 4 on auth/query errors.
- Raw JSONL in `data/raw/`, normalized CSV in `data/clean/`.
