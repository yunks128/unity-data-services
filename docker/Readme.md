- building docker file for Granules Download
        
        docker build -f ./docker/Dockerfile_download_granules.jpl -t cumulus_unity:1.0.0-t1 .
        
- running docker file for Granules Download
    - update the environment variables

        docker-compose -f docker/docker-compose-granules-download.yaml up
        
