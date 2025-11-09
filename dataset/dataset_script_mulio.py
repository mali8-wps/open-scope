import csv
import json
import base64
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import threading

CSV_FILE = "./2025-openrank-top10000.csv"
OUTPUT_FILE = "repos_output.jsonl"
GITHUB_TOKEN = ""

MAX_WORKERS = 10
RETRY_LIMIT = 3
MIN_INTERVAL = 0.1

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

TOPICS_HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.mercy-preview+json"
}

file_lock = threading.Lock()


def fetch_json(url, headers, retry=0):
    try:
        r = requests.get(url, headers=headers, timeout=60)
        if r.status_code == 200:
            return r.json()
        elif r.status_code in (403, 429):
            if retry < RETRY_LIMIT:
                time.sleep(2 ** retry)
                return fetch_json(url, headers, retry + 1)
            return None
        else:
            return None
    except Exception:
        if retry < RETRY_LIMIT:
            time.sleep(2 ** retry)
            return fetch_json(url, headers, retry + 1)
        return None


def get_repo_info(repo_full_name):
    url = f"https://api.github.com/repos/{repo_full_name}"
    data = fetch_json(url, TOPICS_HEADERS)
    if not data:
        return None
    return {
        "description": data.get("description"),
        "homepage_url": data.get("homepage"),
        "topics": data.get("topics", [])
    }


def get_readme(repo_full_name):
    url = f"https://api.github.com/repos/{repo_full_name}/readme"
    data = fetch_json(url, HEADERS)
    if not data or "content" not in data:
        return None
    try:
        readme_bytes = base64.b64decode(data["content"])
        return readme_bytes.decode("utf-8", errors="ignore")
    except:
        return None


def process_repo(row):
    repo_name = row["repo_name"]
    success = True

    info = get_repo_info(repo_name)
    if not info:
        success = False
        info = {"description": None, "homepage_url": None, "topics": []}

    readme_text = get_readme(repo_name)
    if readme_text is None:
        success = False

    result = {
        "repo_id": row["repo_id"],
        "repo_name": repo_name,
        "total_openrank": row["total_openrank"],
        "description": info["description"],
        "homepage_url": info["homepage_url"],
        "topics": info["topics"],
        "readme_text": readme_text,
        "success": success
    }

    with file_lock:
        with open(OUTPUT_FILE, "a", encoding="utf-8") as out_file:
            out_file.write(json.dumps(result, ensure_ascii=False) + "\n")

    print(f"{repo_name}: {'✅ 成功' if success else '❌ 失败'}")

    time.sleep(MIN_INTERVAL)

    return result


def main():
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        pass

    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        reader = list(csv.DictReader(f))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_repo, row) for row in reader]

        for _ in tqdm(as_completed(futures), total=len(futures), desc="Processing repositories"):
            pass

    print(f"✅ All done! Output saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
