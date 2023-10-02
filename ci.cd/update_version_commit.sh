#!/usr/bin/env bash
git status
git diff
git config --local user.email "wai.phyo@jpl.nasa.gov"
git config --local user.name ${GITHUB_TRIGGERING_ACTOR}

current_branch=`git branch --show-current`
temp_branch="chore-version-update"
commit_message="chore: update version + change log"
git checkout -b
git add -u
git commit -m ${commit_message}
git push --force origin $temp_branch
pr_number=`gh pr create --base ${current_branch} --head ${$temp_branch} --title ${commit_message} | grep -oP '#\K\d+'`
echo ${pr_number}
gh pr review $pr_number --approve
gh pr merge $pr_number --squash --merge
git branch -D ${temp_branch}

