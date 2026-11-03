#!/usr/bin/env python3
"""Search arXiv and return structured results with date filtering."""
import urllib.request
import xml.etree.ElementTree as ET
import sys
import json

def search(query, max_results=30, sort_by="submittedDate", sort_order="descending"):
    url = (
        f"https://export.arxiv.org/api/query?"
        f"search_query={query}&max_results={max_results}"
        f"&sortBy={sort_by}&sortOrder={sort_order}"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "HermesAgent/1.0"})
    with urllib.request.urlopen(req) as resp:
        data = resp.read()
    ns = {"a": "http://www.w3.org/2005/Atom"}
    root = ET.fromstring(data)
    papers = []
    for entry in root.findall("a:entry", ns):
        title_el = entry.find("a:title", ns)
        id_el = entry.find("a:id", ns)
        pub_el = entry.find("a:published", ns)
        sum_el = entry.find("a:summary", ns)
        if title_el is None or id_el is None or pub_el is None or sum_el is None:
            continue
        title = title_el.text.strip().replace("\n", " ")
        arxiv_id = id_el.text.strip().split("/abs/")[-1]
        published = pub_el.text[:10]
        authors_list = []
        for a in entry.findall("a:author", ns):
            name_el = a.find("a:name", ns)
            if name_el is not None and name_el.text:
                authors_list.append(name_el.text)
        authors = ", ".join(authors_list)
        summary = sum_el.text.strip()[:400]
        cats_list = []
        for c in entry.findall("a:category", ns):
            term = c.get("term")
            if term:
                cats_list.append(term)
        cats = ", ".join(cats_list)
        papers.append({
            "title": title,
            "arxiv_id": arxiv_id,
            "published": published,
            "authors": authors,
            "summary": summary,
            "categories": cats,
            "pdf_url": f"https://arxiv.org/pdf/{arxiv_id}",
            "abs_url": f"https://arxiv.org/abs/{arxiv_id}",
        })
    return papers

def print_papers(papers, since_date="2026-05-30"):
    recent = [p for p in papers if p["published"] >= since_date]
    print(f"Papers since {since_date}: {len(recent)}/{len(papers)} total")
    for i, p in enumerate(recent):
        print(f"{i+1}. [{p['arxiv_id']}] {p['title']}")
        print(f"   Authors: {p['authors']}")
        print(f"   Published: {p['published']} | Categories: {p['categories']}")
        print(f"   Abstract: {p['summary'][:200]}...")
        print(f"   PDF: {p['pdf_url']}")
        print()

if __name__ == "__main__":
    queries = sys.argv[1:]
    if not queries:
        print("Usage: search_arxiv_digest.py <query1> [query2 ...]")
    for q in queries:
        print(f"\n{'='*80}")
        print(f"SEARCH: {q}")
        print(f"{'='*80}\n")
        papers = search(q)
        print_papers(papers)
