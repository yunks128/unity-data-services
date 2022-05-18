#!/usr/bin/env bash

apt-get update -y && apt-get install zip -y

ZIP_NAME='cumulus_lambda_functions_deployment.zip'
project_root_dir=${GITHUB_WORKSPACE}
zip_file="${project_root_dir}/$ZIP_NAME" ; # save the result file in current working directory

tmp_proj='/tmp/cumulus_lambda_functions'

source_dir=`python3 -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])'`

cd ${source_dir}
rm -rf ${zip_file} && \
zip -r9 ${zip_file} . && \
echo "zipped to ${zip_file}"

cat $GITHUB_ENV
echo "---"
echo ${{ env.ARTIFACT_BASE_NAME }}
software_version=`python3 ${project_root_dir}/setup.py --version`
echo "software_version=${software_version}" >> $GITHUB_ENV
artifact_file_name="${{ env.ARTIFACT_BASE_NAME }}-${{ env.software_version }}.zip"
echo "ARTIFACT_FILE=$artifact_file_name" >> $GITHUB_ENV
cp ""${GITHUB_WORKSPACE}/cumulus_lambda_functions_deployment.zip" "$artifact_file_name"
