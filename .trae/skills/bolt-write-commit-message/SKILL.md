---
name: bolt-write-commit-message
description: Draft or revise a commit message for a Bolt change. Use when the user asks to write, draft, rewrite, or polish a commit message.
---

# Bolt Write Commit Message

Draft commit messages from the actual diff, not from memory or the conversation
alone.

## Required Inputs

Before drafting:

1. Read `AGENTS.md`, `CONTRIBUTING.md`, and `doc/workflow.md`.
2. Inspect the current commit message if amending.
3. Read `git status --short --branch`.
4. Read the full relevant diff, not just file names.
5. Identify the user-visible behavior, bug symptom, or developer workflow change.

If the concrete behavior or symptom cannot be inferred, ask the user before
drafting a message that could mislead reviewers.

## Title

Use a concise conventional commit title:

```text
fix(scope): Description
feat(scope): Description
refactor(scope): Description
test(scope): Description
docs(scope): Description
build(scope): Description
ci(scope): Description
```

The scope is optional. Start the description with a capital letter when it reads
as a sentence fragment. Do not end the title with a period.

## Body

Include only information that helps a reviewer understand the change:

- Lead with the behavior change or problem being fixed.
- Include one concrete anchor for bug fixes, such as an error, wrong result,
  crash, or before/after behavior.
- Explain the mechanism as one concept when it is not obvious from the title.
- Mention intentionally deferred cases only when they affect reviewers or users.
- Mention validation strategy only when it is not obvious from the diff.

Avoid:

- Restating the diff file by file.
- Listing every touched symbol.
- Saying only that tests pass or CI is green.
- Hard-wrapping paragraphs solely to 72 or 80 columns.
- Claiming performance improvement without benchmark evidence.

## Trailer

Every commit message created or edited by TRAE CLI must end with this trailer
exactly once:

```text
Co-authored-by: TRAE CLI <noreply@bytedance.com>
```

Keep existing trailers and append this trailer at the end if it is missing. Keep
one blank line between the commit body and the trailer block.

## Self-Check

Before showing the draft:

- The title type matches the change.
- The message is backed by facts in the diff.
- The first body paragraph explains why the change matters.
- Test or benchmark claims are specific and verified.
- The required trailer appears exactly once.
