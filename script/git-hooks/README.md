# Git Hooks

This directory contains all Git hooks used in Peloton. Git hooks provide
a mechanism to execute custom scripts when important events occur during
your interaction with this Git repository. More details on Git hooks can
be found [here](https://git-scm.com/book/gr/v2/Customizing-Git-Git-Hooks).

### Pre-commit

A pre-commit hook is fired on every commit into your local Git
repository. Peloton's pre-commit hook collects all modified files
(excluding deleted files) and runs them through our source code
validator script in `script/validator/source_validator.py`. *Ideally*,
we should also ensure every commit successfully compiles and, to a
lesser extent, that the commit passes the tests, but this isn't
reasonable for now.

**Installation:** Under the peloton root directory, run
`ln -s ../../script/git-hooks/pre-commit .git/hooks/pre-commit` to
install locally. The pre-commit file should already have executable
permission, but check again to make sure.