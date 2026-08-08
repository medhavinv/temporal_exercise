# Repo rules for Claude

- When work on a feature branch is complete and approved, merge it into the
  default branch (currently `claude/temporal-learning-overview-1tnzys`) and
  push. Do not leave finished work stranded on a side branch.
- After merging, delete the feature branch: locally (`git branch -d`) and on
  the remote (`git push origin --delete <branch>`). If the remote delete is
  rejected (e.g. permissions/branch protection), say so explicitly rather than
  leaving it unmentioned.
