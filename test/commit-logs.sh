#!/bin/bash
set -e # exit with nonzero exit code if anything fails

# clear and re-create the out directory
rm -rf out || exit 0;
mkdir out;

# go to the out directory and create a *new* Git repo
cd out

git init
# inside this git repo we'll pretend to be a new user
git config user.name "Travis CI"
git config user.email "luis.rascao@gmail.com"
git remote add origin "https://${GH_TOKEN}@${GH_REF}" > /dev/null 2>&1
git fetch --all
git checkout gh-pages
git reset --hard origin/gh-pages

# first check if a directory with the same name already exists
if [ -d "${TRAVIS_JOB_NUMBER}" ]; then
  git rm ${TRAVIS_JOB_NUMBER}
fi

# create a directory with the job number
mkdir ${TRAVIS_JOB_NUMBER}

# copy the entire contents of the common test
cp --recursive --force ../logs/* ${TRAVIS_JOB_NUMBER}

# get the markdown success symbol based on the test result
if [ ${TRAVIS_TEST_RESULT} == 1 ]
then
    TEST_RESULT_EMOJI=":x:"
else
    TEST_RESULT_EMOJI=":white_check_mark:"
fi

# add the link to README.md
echo "* $TEST_RESULT_EMOJI [#${TRAVIS_JOB_NUMBER} (OTP ${TRAVIS_OTP_RELEASE})](http://lrascao.github.io/mnesia2/${TRAVIS_JOB_NUMBER})\
 [${TRAVIS_COMMIT}](https://github.com/lrascao/mnesia2/${TRAVIS_COMMIT})" >> README.md
echo >> README.md

# The first and only commit to this new Git repo contains all the
# files present with the commit message "Deploy to GitHub Pages".
git add ${TRAVIS_JOB_NUMBER}
git add README.md
git commit -m "Job ${TRAVIS_JOB_NUMBER} logs (OTP ${TRAVIS_OTP_RELEASE})"

# Force push from the current repo's master branch to the remote
# repo's gh-pages branch. (All previous history on the gh-pages branch
# will be lost, since we are overwriting it.) We redirect any output to
# /dev/null to hide any sensitive credential data that might otherwise be exposed.
git push --quiet origin gh-pages > /dev/null 2>&1
