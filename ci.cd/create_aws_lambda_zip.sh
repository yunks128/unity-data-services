#!/usr/bin/env bash

apt-get update -y && apt-get install zip -y

ZIP_NAME='cumulus_lambda_functions_deployment.zip'
TERRAFORM_ZIP_NAME='terraform_cumulus_lambda_functions_deployment.zip'
project_root_dir=${GITHUB_WORKSPACE}
zip_file="${project_root_dir}/$ZIP_NAME" ; # save the result file in current working directory
terraform_zip_file="${project_root_dir}/$TERRAFORM_ZIP_NAME" ; # save the result file in current working directory

source_dir=`python3 -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])'`

cd ${source_dir}
rm -rf ${zip_file} && \
zip -r9 ${zip_file} . && \
echo "zipped to ${zip_file}"

cd ${project_root_dir}/tf-module/unity-cumulus
mkdir build
cp ${zip_file} build/

cd $project_root_dir/tf-module
zip -r9 ${terraform_zip_file} unity-cumulus

software_version=`python3 ${project_root_dir}/setup.py --version`
echo "software_version=${software_version}" >> ${GITHUB_ENV}
cat ${GITHUB_ENV}
