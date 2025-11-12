import csv
import json
import base64
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import threading
import os

CSV_FILE = "./2025-openrank-top10000.csv"
OUTPUT_FILE = "repos_output.jsonl"
FAILED_FILE = "failed_repos.jsonl"
SUCCESS_FILE = "success_repos.jsonl"

# 具体的token
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
        elif r.status_code == 404:
            return {"_error": "not_found"}
        elif r.status_code in (403, 429):
            reset = r.headers.get("X-RateLimit-Reset")
            remaining = r.headers.get("X-RateLimit-Remaining")
            if remaining == "0" and reset:
                sleep_time = int(reset) - int(time.time()) + 5
                print(f"⚠️ Rate limit reached. Sleeping {sleep_time}s ...")
                time.sleep(max(sleep_time, 10))
            elif retry < RETRY_LIMIT:
                time.sleep(2 ** retry)
                return fetch_json(url, headers, retry + 1)
            return {"_error": f"rate_limit_or_forbidden_{r.status_code}"}
        else:
            return {"_error": f"status_{r.status_code}"}
    except Exception as e:
        if retry < RETRY_LIMIT:
            time.sleep(2 ** retry)
            return fetch_json(url, headers, retry + 1)
        return {"_error": f"exception_{type(e).__name__}: {str(e)}"}

def get_repo_info(repo_full_name):
    url = f"https://api.github.com/repos/{repo_full_name}"
    data = fetch_json(url, TOPICS_HEADERS)
    if not data or "_error" in data:
        return {"_error": data.get("_error") if data else "unknown_error"}
    return {
        "description": data.get("description"),
        "homepage_url": data.get("homepage"),
        "topics": data.get("topics", [])
    }


def get_readme(repo_full_name):
    url = f"https://api.github.com/repos/{repo_full_name}/readme"
    data = fetch_json(url, HEADERS)
    if not data:
        return {"_error": "no_response"}
    if "_error" in data:
        return {"_error": data["_error"]}
    if "content" not in data:
        return {"_error": "no_content_field"}
    try:
        text = base64.b64decode(data["content"]).decode("utf-8", errors="ignore")
        return {"text": text}
    except Exception as e:
        return {"_error": f"decode_error_{type(e).__name__}"}


def load_jsonl(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]
    return []


def save_jsonl(file_path, data):
    with open(file_path, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


def process_repo(row, success_repo_ids):
    repo_name = row["repo_name"]
    repo_id = row["repo_id"]
    success = True
    fail_reason = None

    info = get_repo_info(repo_name)
    if "_error" in info:
        success = False
        fail_reason = info["_error"]
        info = {"description": None, "homepage_url": None, "topics": []}

    readme_data = get_readme(repo_name)
    if "_error" in readme_data:
        # 如果只是没有 README，不算失败
        if readme_data["_error"] != "not_found":
            success = False
            fail_reason = readme_data["_error"]
        readme_text = None
    else:
        readme_text = readme_data["text"]

    result = {
        "repo_id": repo_id,
        "repo_name": repo_name,
        "total_openrank": row["total_openrank"],
        "description": info["description"],
        "homepage_url": info["homepage_url"],
        "topics": info["topics"],
        "readme_text": readme_text,
        "success": success,
        "fail_reason": fail_reason
    }

    with file_lock:
        if success:
            with open(OUTPUT_FILE, "a", encoding="utf-8") as f_out:
                f_out.write(json.dumps(result, ensure_ascii=False) + "\n")
            if repo_id not in success_repo_ids:
                with open(SUCCESS_FILE, "a", encoding="utf-8") as f_succ:
                    f_succ.write(json.dumps({"repo_id": repo_id, "repo_name": repo_name}, ensure_ascii=False) + "\n")
                success_repo_ids.add(repo_id)
        else:
            with open(FAILED_FILE, "a", encoding="utf-8") as f_fail:
                f_fail.write(json.dumps(result, ensure_ascii=False) + "\n")

    print(f"{repo_name}: {'✅ 成功' if success else '❌ 失败'} {f'({fail_reason})' if fail_reason else ''}")
    time.sleep(MIN_INTERVAL)
    return result


def main():
    success_repo_records = load_jsonl(SUCCESS_FILE)
    success_repo_ids = set(r["repo_id"] for r in success_repo_records)

    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        all_rows = list(csv.DictReader(f))

    pending_repos = [row for row in all_rows if row["repo_id"] not in success_repo_ids]

    failed_repos = load_jsonl(FAILED_FILE)

    # 合并待抓取列表，去重
    pending_dict = {row["repo_id"]: row for row in pending_repos}
    for row in failed_repos:
        if row["repo_id"] not in pending_dict:
            pending_dict[row["repo_id"]] = row
    repos_to_process = list(pending_dict.values())

    print(f"总共待处理仓库: {len(repos_to_process)}")

    save_jsonl(FAILED_FILE, [])

    while repos_to_process:
        failed_next_round = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_repo, row, success_repo_ids) for row in repos_to_process]

            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing repositories"):
                result = future.result()
                if not result["success"]:
                    failed_next_round.append({
                        "repo_id": result["repo_id"],
                        "repo_name": result["repo_name"],
                        "total_openrank": result["total_openrank"]
                    })

        if failed_next_round:
            print(f"{len(failed_next_round)} 个仓库失败，将在下一轮重试...")
            save_jsonl(FAILED_FILE, failed_next_round)
            repos_to_process = failed_next_round
            time.sleep(5)
        else:
            break

    print(f"✅ All done! 成功仓库写入 {OUTPUT_FILE}，失败仓库写入 {FAILED_FILE}，成功记录文件 {SUCCESS_FILE}")


if __name__ == "__main__":
    main()
