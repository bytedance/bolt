The Bolt project uses the GitHub workflow overview, which includes some tips and suggestions, such as keeping the local environment
synchronized and submitted upstream. This document provides a workflow for completing Bolt development on GitHub.
<br>

## Step1. Fork Bolt in the cloud

- Visit: https://github.com/bytedance/bolt.

- Click `Fork` button (top right) to establish a cloud-based fork.

	<br>


## Step2. Clone fork to local storage

- Click code > clone


```Plain
$ cd $working_dir
$ git clone https://github.com/$user/bolt
```

- Add your cloned repo to upstream


```Plain
$ cd $working_dir/bolt
$ git remote add upstream https://github.com/bytedance/bolt.git
```

- Use the `git remote -v` command to view the remote warehouse


```Plain
origin    https://github.com/$user/bolt.git (fetch)
origin    https://github.com/$user/bolt.git (push)
upstream  https://github.com/bytedance/bolt (fetch)
upstream  https://github.com/bytedance/bolt (push)
```

## Step3. Sync branch

- Make sure your branch and remote content are consistent


```Plain
$ cd $working_dir/bolt
$ git checkout main
$ git fetch upstream
$ git rebase upstream/main
$ git push origin main
```

## Step4 Create a branch

- Create branch based on master


`$ git checkout -b myfeature`

## Step5 Setting up the development environment

Run `scripts/setup-dev-env.sh`to prepare dependencies.

## Step6 Modify content or code And Test

- Now you can modify the content or code in your newly created branch

- Test your changes, more commands please refer to [Building and Testing](../CONTRIBUTING.md#building-and-testing)


```Plain
$ make release_with_test
```

### Pre-Submission Checklist

Before opening a pull request, please complete the following self-check:

- **Builds Successfully**: `make release_with_test` or `make debug_with_test` completes without errors.

- **Tests Pass**: `make unittest[_release]` reports no failures. New tests have been added where necessary.

- **Style is Clean**: `make clang-format-check` passes. Run `clang-format -i` locally if needed.

- **Static Analysis is Clean**: `scripts/run-clang-tidy.py` introduces no new warnings (or they have been reasonably justified).

- **License Headers**: New source files include the `license.header`.

- **Changes are Focused**: The PR is small and focused, avoiding mixed concerns (e.g., refactoring and feature work in the same PR).


### Benchmark Suggestions

If your changes may impact performance or resource usage, please include comparison data in your PR.

- Build the benchmark targets:


```Bash
make benchmarks-basic-build
# or
make benchmarks-build
```

- Describe the test data, scenario, metrics, and conclusions. Provide reproducible commands and configurations. For framework-related changes (e.g., Spark/Gluten), specify the versions and build options used.


## Step7 Commit

- Commit your changes


```Plain
$ git add <filename>
$ git commit -s -am "$add a comment" (commit with signed-off-by)
```

- After commit your changes, you may need to modify and submit several rounds. You can refer to the following commands


```Plain
$ git add <filename> (used to add one file)
git add -A (add all changes, including new/delete/modified files)
git commit -a -m "$add a comment" (add and commit modified and deleted files)
git add -u (add modified and deleted files, not include new files)
git add . (add new and modified files, not including deleted files)
git commit --amend -m "$add a comment" (modify the last commit)
```

## Step8 Push

- After completing the change, you need to push the changed content to the remote repo of your fork.


`$ git push origin myfeature`
<br>

## Step9 pull request

- Visit the repository of your fork https://github.com/$user/bolt.

- Click `Compare & pull request`

- Sign CLA https://cla-assistant.io/bytedance/bolt for your first PR

- Fill out the [Pull Request](https://github.com/bytedance/bolt/compare) completely.

	###   Commit Message Suggestions

	- Use a concise and clear title in English, preferably following the "type: subject" convention:
		- `feat: add X to Y`

		- `fix: correct Z when ...`

		- `docs: update README for ...`

		- `test: add unit tests for ...`

		- `build: adjust Makefile option ...`

		- `ci: run clang-format check in ...`

	- Commits should be atomic to facilitate review and cherry-picking.


	###   Pull Request Description Suggestions

	- **Should link to an issue** and describe the problem and motivation.

	- Provide a **technical summary** of the changes, including impact and risk analysis.

	- Explain your **test coverage** (new/modified unit tests, manual verification steps).

	- If performance is affected, include **benchmark data** and a comparison methodology (see "Benchmark Suggestions" below).


<br>

## Step10: Review and Merge

After the PR is submitted, it will be reviewed by at least 2 reviewers.
The reviewer need to confirm that it is correct, and the maintainers of Bolt will merge your pull request after accepting the final changes.

### Review Process and Best Practices

- Maintainers will review your PR as soon as possible. Please respond to feedback promptly and push new commits to the same branch.

- Keeping PRs small and focused significantly improves review and merge velocity.

- Be mindful of potential compatibility issues or behavioral changes. Clearly document them and provide migration paths or feature flags if necessary.


## Step11: Wait and check the CI passes

Once the PR is submitted, bolt will automatically trigger a CI where the Required check must pass, and if a check does not pass, you can click on the Details link to see the details.
