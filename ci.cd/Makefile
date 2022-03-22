all: build_lambda upload_lambda update_lambda_function

build_lambda:
	docker run --rm -v `PWD`:"/usr/src/app/cumulus_lambda_functions":z -w "/usr/src/app/cumulus_lambda_functions" cae-artifactory.jpl.nasa.gov:17001/python:3.7 ci.cd/create_s3_zip.sh

upload_lambda:
	aws --profile saml-pub s3 cp cumulus_lambda_functions_deployment.zip s3://am-uds-dev-cumulus-tf-state/unity_cumulus_lambda/

update_lambda_function:
	aws --profile saml-pub lambda update-function-code --s3-key unity_cumulus_lambda/cumulus_lambda_functions_deployment.zip --s3-bucket am-uds-dev-cumulus-tf-state --function-name arn:aws:lambda:us-west-2:884500545225:function:Test1 --publish
