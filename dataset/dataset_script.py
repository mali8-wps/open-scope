import csv
import json
import base64
import requests
import os
from tqdm import tqdm

CSV_FILE = "./2025-openrank-top10000.csv"
OUTPUT_FILE = "repos_output.jsonl"

GITHUB_TOKEN = ""

headers = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

topics_headers = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.mercy-preview+json"
}

def get_repo_info(repo_full_name):
    url = f"https://api.github.com/repos/{repo_full_name}"
    r = requests.get(url, headers=topics_headers)
    if r.status_code != 200:
        return None
    data = r.json()
    return {
        "description": data.get("description"),
        "homepage_url": data.get("homepage"),
        "topics": data.get("topics", [])
    }


def get_readme(repo_full_name):
    url = f"https://api.github.com/repos/{repo_full_name}/readme"
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        return None
    data = r.json()
    if "content" in data:
        try:
            readme_bytes = base64.b64decode(data["content"])
            return readme_bytes.decode("utf-8", errors="ignore")
        except:
            return None
    return None

with open(CSV_FILE, newline='', encoding='utf-8') as f, \
     open(OUTPUT_FILE, "w", encoding='utf-8') as out:

    reader = csv.DictReader(f)
    
    for row in tqdm(reader, desc="Processing repositories"):
        repo_name = row["repo_name"]

        info = get_repo_info(repo_name)
        if info is None:
            continue
        
        readme_text = get_readme(repo_name)

        result = {
            "repo_id": row["repo_id"],
            "repo_name": repo_name,
            "total_openrank": row["total_openrank"],
            "description": info["description"],
            "homepage_url": info["homepage_url"],
            "topics": info["topics"],
            "readme_text": readme_text
        }

        out.write(json.dumps(result, ensure_ascii=False) + "\n")

print(f"✅ All done! Output saved to {OUTPUT_FILE}")