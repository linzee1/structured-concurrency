# AGENTS.md - parallel-in-scope-demo

## Project

Standalone Java 8 example project consuming the published `parallel-in-scope`
artifact. Must build independently from the repository's root Maven project.

## Commands

Run from `demo/`.

```bash
mvn test                       # all tests
mvn test -Dtest=<TestClass>    # targeted test
mvn dependency:tree            # dependency audit
mvn exec:java -Dexec.mainClass=demo.basic.BasicParDemo   # run one demo
mvn verify -Prun-all-demos                               # run all demos
```

## Critical Rules

- Depend on the published library artifact; never reference the root project's
  source tree.
- Use only the public APIs allowed by `architecture-constraints.md`.
- Tests in `src/test/java/demo/article/` back the articles in
  `docs/zh-CN/articles/`; add or update a test when an article changes
  observable behavior.
- Keep article snippets focused; do not repeat imports when the linked complete
  test contains them.
- Update `scripts/run-demos.sh` when adding a runnable main class.

## Done Criteria

- Run `mvn test` before finishing changes that affect the demo module (a
  targeted test suffices while iterating).

## Reference Documents

- `architecture-constraints.md` - Read before changing dependencies, package
  names, imports, or module boundaries.
- `README.en.md` - Read when adding a runnable demo or changing user-facing demo
  navigation.
