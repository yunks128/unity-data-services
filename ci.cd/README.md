### How to run

Create a zip file using a base docker. Run this from root-dir:

        docker run --rm -v `PWD`:"/usr/src/app/cumulus_lambda_functions":z -w "/usr/src/app/cumulus_lambda_functions" cae-artifactory.jpl.nasa.gov:17001/python:3.7 ci.cd/create_s3_zip.sh
To debug a build script:

        docker run --rm -v `PWD`:"/usr/src/app/cumulus_lambda_functions":z -w "/usr/src/app/cumulus_lambda_functions" -it cae-artifactory.jpl.nasa.gov:17001/python:3.7 /bin/bash
        
