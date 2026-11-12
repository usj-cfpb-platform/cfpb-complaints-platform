# Contributing

How our team of three works so every contribution is visible and reviewed.

## 1. Identity
- Commit with your own GitHub account.
- `git config user.email` must be an email **verified** on your GitHub account
  (we use each member's `@users.noreply.github.com` address).

## 2. Branches
- `main` is protected: no direct pushes, every change goes through a pull request.
- One branch per issue: `<type>/<short-topic>`, e.g. `feat/ingestion-api-pagination`,
  `fix/spark-date-parsing`, `docs/runbook-reset`.

## 3. Commits (Conventional Commits)
- Small and frequent; commit every working day.
- Prefixes: `feat:`, `fix:`, `test:`, `docs:`, `chore:`, `refactor:`, `ci:`.
- Pair work: add a trailer `Co-authored-by: Name <noreply-email>`.
- AI-generated code is allowed but its author must read it, test it and be able to explain it.

## 4. Pull requests
- Fill in the PR template: what, why, how tested, evidence, handoff notes.
- Link the issue: `Closes #12`.
- At least **one approving review from another member**. Reviews contain real comments
  (questions, risks, suggestions), not only "LGTM".
- CI must pass before merge. Merge with a merge commit (history keeps each commit).

## 5. Issues and board
- Every task is an issue, assigned to its owner, labelled by layer
  (`layer:ingestion`, `layer:processing`, `layer:database`, `layer:analysis`,
  `layer:dashboard`, `layer:infra`, `docs`, `test`).
- Milestones: **Week 1 - pipeline end-to-end**, **Week 2 - final release**.

## 6. Turns
Mhamad -> Maroun -> Ali -> Mhamad -> Maroun -> Ali. Only the member whose turn it is pushes
features; the other two review, write report sections and prepare their next turn.
Each turn ends with a merged PR containing a handoff note.

## 7. Contribution logs
Update `docs/contributions/<name>.md` every working day.
