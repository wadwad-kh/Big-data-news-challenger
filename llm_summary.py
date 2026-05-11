import os
import requests


def clean_keywords(keywords):
    cleaned = []

    for item in keywords:
        if isinstance(item, dict):
            word = item.get("word", "")
        else:
            word = str(item)

        word = word.strip()

        if word != "":
            cleaned.append(word)

    return cleaned


def limit_words(text, max_words=80):
    words = text.split()

    if len(words) <= max_words:
        return text

    return " ".join(words[:max_words]) + "..."


def fallback_summary(keywords, lang_code="en"):
    keywords = clean_keywords(keywords)

    if len(keywords) == 0:
        if lang_code == "ar":
            return "لا توجد مواضيع إخبارية واضحة حتى الآن لأن جدول الكلمات المفتاحية فارغ."
        return "No clear news themes are available yet because the keyword table is empty."

    joined = ", ".join(keywords[:10])

    if lang_code == "ar":
        text = (
            "يركز نبض الأخبار الحالي على هذه الكلمات المفتاحية: "
            + joined
            + ". وتشير هذه الكلمات إلى قصص متعلقة بالسياسة والأحداث العالمية والأمن والشؤون العامة."
        )
    else:
        text = (
            "The current news pulse is mainly focused on these keywords: "
            + joined
            + ". These terms suggest developing stories across politics, global events, security, and public affairs."
        )

    return limit_words(text, 80)


def generate_summary(top_words, lang_code="en"):
    keywords = clean_keywords(top_words)

    if len(keywords) == 0:
        return fallback_summary(keywords, lang_code)

    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        return fallback_summary(keywords, lang_code)

    language_prompt = "Arabic" if lang_code == "ar" else "English"

    prompt = (
        "Write one paragraph in "
        + language_prompt
        + ", no more than 80 words, summarizing the current news themes. "
        + "Use these keywords: "
        + ", ".join(keywords[:15])
        + ". Mention at least three named storylines if possible. "
        + "Do not use bullet points."
    )

    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": "Bearer " + api_key,
                "Content-Type": "application/json"
            },
            json={
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.3,
                "max_tokens": 140
            },
            timeout=15
        )

        response.raise_for_status()

        data = response.json()
        summary = data["choices"][0]["message"]["content"].strip()

        return limit_words(summary, 80)

    except Exception:
        return fallback_summary(keywords, lang_code)