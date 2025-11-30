import os
import time
import re
import json
import hashlib
from collections import deque
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://data.politie.nl/#/Politie/nl/navigatieScherm/thema"

HEADERS = {
	"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
	"Accept": "application/json, text/javascript, */*; q=0.01",
	"Accept-Language": "en-US,en;q=0.9",
	"Cache-Control": "no-cache",
}


def get_soup(url, sleep=1.0):
	time.sleep(sleep)
	resp = requests.get(url, headers=HEADERS, timeout=15)
	resp.raise_for_status()
	return BeautifulSoup(resp.text, "html.parser")


def _extract_keyword_info(text, keyword):
	k = keyword.lower()
	contexts = []
	sentences = []
	for s in re.split(r'(?<=[.!?])\s+', text):
		if k in s.lower():
			sentences.append(s.strip())
	for m in re.finditer(re.escape(keyword), text, flags=re.IGNORECASE):
		start = max(0, m.start() - 100)
		end = min(len(text), m.end() + 100)
		contexts.append(text[start:end].strip())
	# dedup
	def _uniq(seq):
		seen = set(); out = []
		for x in seq:
			if x not in seen:
				seen.add(x); out.append(x)
		return out
	return {
		"occurrences": len(contexts),
		"contexts": _uniq(contexts),
		"sentences": _uniq(sentences),
	}


def _is_same_site(url):
	try:
		return urlparse(url).netloc.endswith("politie.nl")
	except Exception:
		return False


def _extract_dataset_id_from_url(url):
	# handle fragment-based single page app URLs too
	m = re.search(r"/dataset/([^/?#]+)", url)
	if m:
		return m.group(1)
	m = re.search(r"#/.*?/dataset/([^/?#]+)", url)
	if m:
		return m.group(1)
	return None


def scrape_dataset_from_url(dataset_url, keyword="vuurwerk", output_csv=None, save_json=True, json_subdir="politie_articles", max_bytes=200000):
	"""Use CKAN API (package_show + optional datastore) to inspect dataset resources for the keyword.

	Downloads each resource up to `max_bytes` and scans for `keyword`. If the resource has a CKAN datastore
	(structured data) the function attempts to call the `datastore_search` API and scan returned records.
	"""
	dataset_id = _extract_dataset_id_from_url(dataset_url)
	if not dataset_id:
		print(f"[ERROR] Could not find dataset id in URL: {dataset_url}")
		return

	print(f"[INFO] Scraping dataset {dataset_id} for keyword '{keyword}'")
	api_base = "https://data.politie.nl/api/3/action"
	pkg_url = f"{api_base}/package_show?id={dataset_id}"
	print(f"[DEBUG] Calling: {pkg_url}")
	try:
		resp = requests.get(pkg_url, headers=HEADERS, timeout=15)
		print(f"[DEBUG] Response status: {resp.status_code}, content-type: {resp.headers.get('content-type')}")
		if resp.status_code == 200 and resp.text:
			print(f"[DEBUG] First 300 chars: {resp.text[:300]}")
		resp.raise_for_status()
		pkg = resp.json()
	except Exception as e:
		print(f"[WARN] package_show failed for {dataset_id}: {e}. Trying package_search fallback")
		# Fallback: try package_search with the dataset id or keyword
		try:
			search_url = f"{api_base}/package_search?q={dataset_id}"
			sr = requests.get(search_url, headers=HEADERS, timeout=15)
			sr.raise_for_status()
			sp = sr.json()
			if sp.get("success") and sp.get("result") and sp["result"].get("results"):
				# pick first matching result
				result = sp["result"]["results"][0]
			else:
				print(f"[ERROR] package_search fallback found no results for {dataset_id}")
				return
		except Exception as e2:
			print(f"[WARN] package_search fallback also failed: {e2}")
			# Try fetching the dataset page directly and look for resource links in HTML
			try:
				page_url = f"https://data.politie.nl/dataset/{dataset_id}"
				pr = requests.get(page_url, headers=HEADERS, timeout=15)
				pr.raise_for_status()
				soup = BeautifulSoup(pr.text, "html.parser")
				# collect candidate resource links
				candidate_links = set()
				for a in soup.find_all("a", href=True):
					href = a.get("href")
					if href and ("/resource/" in href or href.lower().endswith(('.csv', '.json', '.zip')) or 'download' in href.lower()):
						candidate_links.add(urljoin(page_url, href))
				# Build a minimal fake "result" with discovered resources
				resources = []
				for i, l in enumerate(candidate_links, start=1):
					resources.append({"id": f"link{i}", "name": os.path.basename(l), "url": l, "format": os.path.splitext(l)[1].lstrip('.')})
				result = {"title": dataset_id, "resources": resources}
				print(f"[INFO] Found {len(resources)} candidate resource links on dataset page")
			except Exception as e3:
				print(f"[ERROR] dataset page fallback failed: {e3}")
				return

	else:
		if not pkg.get("success"):
			print(f"[ERROR] package_show returned success=false for {dataset_id}")
			return
		result = pkg.get("result", {})
	dataset_title = result.get("title") or result.get("name") or dataset_id
	resources = result.get("resources", [])

	project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
	if output_csv is None:
		output_csv = os.path.join(project_root, "output_scrapers", "politie_vuurwerk.csv")
	output_dir = os.path.dirname(output_csv)
	os.makedirs(output_dir, exist_ok=True)
	json_dir = os.path.join(output_dir, json_subdir)
	if save_json:
		os.makedirs(json_dir, exist_ok=True)

	rows = []

	for res in resources:
		res_name = res.get("name") or res.get("id")
		res_format = (res.get("format") or "").lower()
		res_url = res.get("url") or res.get("access_url")
		print(f"[INFO] Inspect resource '{res_name}' ({res_format}) -> {res_url}")

		# First, if datastore is active, try datastore_search (structured records)
		if res.get("datastore_active") and res.get("id"):
			ds_url = f"{api_base}/datastore_search?resource_id={res.get('id')}&limit=1000"
			try:
				dr = requests.get(ds_url, headers=HEADERS, timeout=20)
				dr.raise_for_status()
				data = dr.json()
				if data.get("success") and data.get("result"):
					records = data["result"].get("records", [])
					for rec in records:
						text = json.dumps(rec, ensure_ascii=False)
						if keyword.lower() in text.lower():
							kw = _extract_keyword_info(text, keyword)
							row = {
								"dataset": dataset_title,
								"resource": res_name,
								"resource_url": res_url,
								"snippet": kw["contexts"][0] if kw["contexts"] else text[:200],
								"occurrences": kw["occurrences"],
								"contexts": kw["contexts"],
							}
							rows.append(row)
							if save_json:
								sha = hashlib.sha1((res_url or "").encode("utf-8")).hexdigest()[:8]
								fname = f"{dataset_id}_{res.get('id')}_{sha}.json"
								fp = os.path.join(json_dir, fname)
								try:
									with open(fp, "w", encoding="utf-8") as jf:
										json.dump({"dataset": dataset_title, "resource": res, "matches": row}, jf, ensure_ascii=False, indent=2)
								except Exception as e:
									print(f"[WARN] could not write JSON {fp}: {e}")
			except Exception as e:
				print(f"[WARN] datastore_search failed for resource {res.get('id')}: {e}")

		# If resource URL is present, stream a limited amount and scan for keyword
		if res_url and res_format in ("csv", "json", "txt", "xml") or res_url:
			try:
				r = requests.get(res_url, headers=HEADERS, stream=True, timeout=20)
				r.raise_for_status()
				bytes_read = 0
				chunks = []
				for chunk in r.iter_content(chunk_size=8192):
					if not chunk:
						break
					chunks.append(chunk)
					bytes_read += len(chunk)
					if bytes_read >= max_bytes:
						break
				text = b"".join(chunks).decode(r.encoding or "utf-8", errors="replace")
				if keyword.lower() in text.lower():
					kw = _extract_keyword_info(text, keyword)
					row = {
						"dataset": dataset_title,
						"resource": res_name,
						"resource_url": res_url,
						"snippet": kw["contexts"][0] if kw["contexts"] else text[:200],
						"occurrences": kw["occurrences"],
						"contexts": kw["contexts"],
					}
					rows.append(row)
					if save_json:
						sha = hashlib.sha1((res_url or "").encode("utf-8")).hexdigest()[:8]
						fname = f"{dataset_id}_{res.get('id') or res_name}_{sha}.json"
						fp = os.path.join(json_dir, fname)
						try:
							with open(fp, "w", encoding="utf-8") as jf:
								json.dump({"dataset": dataset_title, "resource": res, "matches": row}, jf, ensure_ascii=False, indent=2)
						except Exception as e:
							print(f"[WARN] could not write JSON {fp}: {e}")
			except Exception as e:
				print(f"[WARN] could not fetch resource {res_url}: {e}")

	# write summary CSV for dataset search results
	fieldnames = ["dataset", "resource", "resource_url", "snippet", "occurrences", "contexts"]
	with open(output_csv, "w", newline="", encoding="utf-8") as f:
		import csv
		writer = csv.DictWriter(f, fieldnames=fieldnames)
		writer.writeheader()
		for r in rows:
			writer.writerow({
				"dataset": r.get("dataset", ""),
				"resource": r.get("resource", ""),
				"resource_url": r.get("resource_url", ""),
				"snippet": r.get("snippet", ""),
				"occurrences": r.get("occurrences", 0),
				"contexts": json.dumps(r.get("contexts", []), ensure_ascii=False),
			})

	print(f"[DONE] Found {len(rows)} matching resources/records for dataset {dataset_id}. CSV: {output_csv}")


def scrape_politie_vuurwerk(keyword="Vuurwerk", output_csv=None, max_total_pages=100, max_depth=2, max_links_per_page=10, save_json=True, json_subdir="politie_articles"):
	"""Crawl data.politie.nl starting from the base URL and save pages containing `keyword`.

	This is a gentle, generic crawler — it doesn't rely on a site search API but follows internal links.
	"""
	if output_csv is None:
		project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
		output_csv = os.path.join(project_root, "output_scrapers", "politie_vuurwerk.csv")

	output_dir = os.path.dirname(output_csv)
	os.makedirs(output_dir, exist_ok=True)

	json_dir = os.path.join(output_dir, json_subdir)
	if save_json:
		os.makedirs(json_dir, exist_ok=True)

	rows = []
	seen = set()
	queue = deque()
	queue.append((BASE_URL, 0))

	while queue and len(seen) < max_total_pages:
		url, depth = queue.popleft()
		if url in seen:
			continue
		try:
			soup = get_soup(url, sleep=1.0)
		except Exception as e:
			print(f"[WARN] could not fetch {url}: {e}")
			seen.add(url)
			continue

		seen.add(url)

		# extract text and title
		title_tag = soup.find("h1") or soup.find("title")
		title = title_tag.get_text(strip=True) if title_tag else ""
		article_node = soup.find("article") or soup.find("main") or soup
		full_text = article_node.get_text(" ", strip=True)

		if keyword.lower() in full_text.lower():
			kw = _extract_keyword_info(full_text, keyword)
			row = {
				"title": title,
				"url": url,
				"snippet": kw["contexts"][0] if kw["contexts"] else full_text[:200],
				"full_text": full_text,
				"keyword_occurrences": kw["occurrences"],
				"keyword_contexts": kw["contexts"],
				"keyword_sentences": kw["sentences"],
			}
			rows.append(row)

			# save JSON
			if save_json:
				sha = hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]
				idx = len(rows)
				slug = re.sub(r"[^0-9a-zA-Z_-]", "_", title)[:40] or "page"
				fname = f"{idx:04d}_{slug}_{sha}.json"
				fp = os.path.join(json_dir, fname)
				try:
					with open(fp, "w", encoding="utf-8") as jf:
						json.dump(row, jf, ensure_ascii=False, indent=2)
				except Exception as e:
					print(f"[WARN] could not write JSON {fp}: {e}")

		# enqueue internal links
		if depth < max_depth:
			links = []
			for a in soup.find_all("a", href=True):
				href = a.get("href")
				full = urljoin(url, href)
				if not _is_same_site(full):
					continue
				if full in seen:
					continue
				links.append(full)
			# limit and enqueue
			for l in links[:max_links_per_page]:
				queue.append((l, depth + 1))

	# write CSV
	fieldnames = ["title", "url", "snippet", "full_text", "keyword_occurrences", "keyword_contexts", "keyword_sentences"]
	with open(output_csv, "w", newline="", encoding="utf-8") as f:
		import csv
		writer = csv.DictWriter(f, fieldnames=fieldnames)
		writer.writeheader()
		for r in rows:
			writer.writerow({
				"title": r["title"],
				"url": r["url"],
				"snippet": r.get("snippet", ""),
				"full_text": r.get("full_text", ""),
				"keyword_occurrences": r.get("keyword_occurrences", 0),
				"keyword_contexts": json.dumps(r.get("keyword_contexts", []), ensure_ascii=False),
				"keyword_sentences": json.dumps(r.get("keyword_sentences", []), ensure_ascii=False),
			})

	print(f"[DONE] {len(rows)} pages containing '{keyword}' saved to {output_csv} and JSONs in {json_dir}")


if __name__ == "__main__":
	# gentle defaults
	# If you have a dataset URL (single-page app with fragment), try the CKAN API for that dataset
	sample_dataset_url = "https://data.politie.nl/#/Politie/nl/dataset/47025NED/table?ts=1764508892319"
	try:
		scrape_dataset_from_url(sample_dataset_url, keyword="vuurwerk")
	except NameError:
		# function not defined yet (older script version). fallback to site crawl
		scrape_politie_vuurwerk(keyword="vuurwerk", max_total_pages=50, max_depth=1, max_links_per_page=10)

