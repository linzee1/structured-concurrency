# AGENTS.md

## Scope

These instructions apply to every Architecture Decision Record in `adr/`.
An ADR records why an important decision was made, not general documentation,
temporary analysis, or a progress log.

## File And Status Rules

- Name new records `NNNN-kebab-case-title.md` using the next sequential number.
- Use one record for one decision.
- Include `Status`, `Date`, `Decision scope`, and `Supersedes` metadata.
- Use these statuses: `Proposed`, `Accepted`, `Deprecated`, or `Superseded`.
- Do not rewrite an accepted decision when the decision changes. Add a new ADR,
  mark the old record `Superseded`, and link both records.

## Required Content

Every ADR must explain:

1. `Context`: the problem, constraints, and forces that require a decision.
2. `Decision`: the chosen approach and any enforceable rules.
3. `Alternatives Considered`: credible options and why they were rejected.
4. `Consequences`: positive effects, costs, risks, and limits.

Add implementation mapping or reconsideration criteria only when they help
future maintainers apply or revisit the decision.

## Content Boundaries

- Keep durable architectural reasoning here.
- Keep test counts, command output, active failures, and temporary investigation
  details in reports outside `adr/`; link them with relative paths when useful.
- Use concrete project types and paths, but do not duplicate source code or
  README material.
- Match the existing ADR language and terminology.
- Do not add ADRs to README or MkDocs navigation unless explicitly requested.

## Validation

- Check relative links and referenced files.
- Java tests are not required for documentation-only ADR edits.
