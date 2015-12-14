# Contributing to the NVM Library

Here you'll find instructions on how to contribute to the NVM Library.

Your contributions are most welcome!  You'll find it is best to begin
with a conversation about your changes, rather than just writing a bunch
of code and contributing it out of the blue.
There are several good ways to suggest new features, offer to add a feature,
or just begin a dialog about the NVM Library:

* Open an issue in our [GitHub Issues Database](https://github.com/pmem/issues/issues)
* Suggest a feature, ask a question, start a discussion, etc. in our [pmem Google group](http://groups.google.com/group/pmem)
* Chat with members of the NVM Library team real-time on the **#pmem** IRC channel on [OFTC](http://www.oftc.net)

**NOTE: If you do decide to implement code changes and contribute them,
please make sure you agree your contribution can be made available
under the [BSD-style License used for the NVM Library](https://github.com/pmem/nvml/blob/master/LICENSE).**

### Code Contributions

Please feel free to use the forums mentioned above to ask
for comments & questions on your code before submitting
a pull request.  The NVM Library project uses the common
*fork and merge* workflow used by most GitHub-hosted projects.
The [Git Workflow blog article](http://pmem.io/2014/09/09/git-workflow.html)
describes our workflow in more detail.

If you are actively working on an NVM Library feature, please let other
developers know by [creating an issue](https://github.com/pmem/issues/issues).
Use the label `Type: Feature` and assign it to yourself (due to the way
GitHub permissions work, you may need to ask a team member to assign it to you).

### Bug Reports

Bugs for the NVM Library project are tracked in our [GitHub Issues Database](https://github.com/pmem/issues/issues).

When creating a bug report issue, please provide the following information:

#### Milestone field

Put the release name of the version of NVML running when the
bug was discovered in the Milestone field.  If you saw this bug
in multiple NVML versions, please put the most recent version in
Milestone and list the others in a bug comment.
- Stable release names are in the form `#.#` (where `#` represents an integer); for example `0.3`.
- Release names from working versions look like `#.#+b#` (adding a build #) or `#.#-rc#` (adding a release candidate number)
If NVML was built from source, the version number can be retrieved
from git using this command: `git describe`

For binary NVML releases, use the entire package name.
For RPMs, use `rpm -q nvml` to display the name.
For Deb packages, run `dpkg-query -W nvml` and use the
second (version) string.

#### Type: Bug label

Assign the `Type: Bug` label to the issue
(see [GitHub Help](https://help.github.com/articles/applying-labels-to-issues-and-pull-requests) for details).

#### Priority label

Optionally, assign one of the Priority labels (P1, P2, ...).
The Priority attribute describes the urgency to resolve a defect
and establishes the time frame for providing a verified resolution.
These Priority labels are defined as:

* **P1**: Showstopper bug, requiring resolution before the next release of the library.
* **P2**: High-priority bug, requiring resolution although it may be decided that the bug does not prevent the next release of the library.
* **P3**: Medium-priority bug.  The expectation is that the bug will be evaluated and a plan will be made for when the bug will be resolved.
* **P4**: Low-priority bug, the least urgent.  Fixed as resources are available.

Then describe the bug in the comment fields.

#### Type: Feature label

Assign the `Type: Feature` label to the issue, then describe the feature request in comment fields.
