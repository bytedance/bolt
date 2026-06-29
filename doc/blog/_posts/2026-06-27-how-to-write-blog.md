---
layout: post
title: "How to Write a Blog Post"
date: 2026-06-27
author: "Bolt Community"
parent: Blog
nav_order: 1
---

Use this guide to create, format, and check a Bolt Blog post before submitting it.
{: .note }

## 1. Choose One Topic

Start with one clear topic. Good topics include:

- A design note for a Bolt component.
- A performance finding with data.
- A debugging story with a clear root cause.
- A release note for a user-visible change.
- A practical guide for using or developing Bolt.

Avoid mixing unrelated updates in one post.

## 2. Create the Post File

Add the post under:

```text
doc/blog/_posts/
```

Use this file name format:

```text
YYYY-MM-DD-lowercase-slug.md
```

Example:

```text
2026-06-27-how-to-write-blog.md
```

The date is part of the post URL, so choose it carefully.

## 3. Add Front Matter

Start every post with this block:

```yaml
---
layout: post
title: "Post Title"
date: YYYY-MM-DD
author: "Your Name"
parent: Blog
nav_order: 1
---
```

Use these values consistently:

| Field | Required value | Notes |
| --- | --- | --- |
| `layout` | `post` | Required for the post layout. |
| `title` | Quoted post title | Keep it clear and specific. |
| `date` | `YYYY-MM-DD` | Must match the file name date. |
| `author` | Quoted author name | Use a person or team name. |
| `parent` | `Blog` | Required for sidebar navigation. |
| `nav_order` | Integer | Controls the post order in the sidebar. |

Do not use `has_children` in posts.

## 4. Write the Content

Use a simple structure:

1. Start with the key takeaway.
2. Explain the problem or motivation briefly.
3. Describe the important details.
4. Add commands, code, data, or examples only when they help.
5. End with the result, impact, or next action.

Write for engineers who want to understand the change quickly.

| Do | Avoid |
| --- | --- |
| Start with the key takeaway. | Mixing unrelated updates. |
| Be specific about modules, workloads, versions, or configurations. | Vague claims such as "it is faster" without context. |
| Use short sections, short paragraphs, and concrete examples. | Long background sections before the main point. |
| Include data or reproduction details for performance and behavior claims. | Placeholder notes such as `TODO`, `TBD`, or "will update later". |
| Use commands and code blocks only when they are useful and runnable. | Code snippets that cannot be copied or understood. |

## 5. Run Local Checks

Run these commands from the repository root:

```bash
python3 scripts/normalize_blog_posts.py --check
npx markdownlint-cli2@0.17.2 "doc/blog/**/*.md" "#doc/blog/vendor/**"
```

If the normalize check fails, run:

```bash
python3 scripts/normalize_blog_posts.py
```

Review the updated file before committing.

## Final Checklist

Before opening a PR, check that:

- [ ] The file name uses `YYYY-MM-DD-lowercase-slug.md`.
- [ ] The front matter is complete and normalized.
- [ ] The post has one clear topic.
- [ ] Commands and code blocks are useful and runnable.
- [ ] Performance or behavior claims include enough context to understand them.
- [ ] The local checks pass.
