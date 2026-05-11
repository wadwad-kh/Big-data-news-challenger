import os
import json
import time
from datetime import datetime
import feedparser

INCOMING = "data/incoming"

FEEDS = {
    "BBC World": "http://feeds.bbci.co.uk/news/world/rss.xml",
    "CNN World": "http://rss.cnn.com/rss/edition_world.rss",
    "Al Jazeera": "https://www.aljazeera.com/xml/rss/all.xml",
    "NPR World": "https://feeds.npr.org/1004/rss.xml",
    "Google News": "https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en"
}

os.makedirs(INCOMING, exist_ok=True)


def get_time_now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def pull_once(tick):
    rows = []

    for source, feed_url in FEEDS.items():
        try:
            feed = feedparser.parse(feed_url)

            for entry in feed.entries[:10]:
                title = entry.get("title", "").strip()
                url = entry.get("link", "").strip()

                if title == "" or url == "":
                    continue

                row = {
                    "source": source,
                    "title": title,
                    "url": url,
                    "ts": get_time_now()
                }

                rows.append(row)

        except Exception as e:
            print("Feed failed:", source, e)

    file_name = f"batch_{tick}_{datetime.now().strftime('%H%M%S')}.json"
    file_path = os.path.join(INCOMING, file_name)

    with open(file_path, "w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"Wrote {len(rows)} headlines to {file_path}")


if __name__ == "__main__":
    tick = 0

    while True:
        pull_once(tick)
        tick += 1
        time.sleep(60)