## Terraform Guide on what are being deployed as there are various components. 

### REST API
- Source: https://www.figma.com/file/dRmKfOzPC2NKACc0NL29bT/DAPA-API-Infrastructure
![DAPA API Infrastructure.png](DAPA%20API%20Infrastructure.png)
### Auto Ingestion Workflow
- Source: https://www.figma.com/file/5kI41JOXP1WPuC4veGCEgm/auto-ingestion-workflow-infrastructure
![auto-ingestion workflow infrastructure.png](auto-ingestion%20workflow%20infrastructure.png)

### CMR metadata generators
- 3 Lambdas which are used in CNM Step function definitions stored in cumulus-template-deploy terraform. 
- Example: STAC [Metadata](https://github.jpl.nasa.gov/unity-uds/cumulus-template-deploy/blob/master/cumulus-tf/catalog_granule_workflow.asl.json#L216) Extraction
### Custom Metadata Ingestion
- Source: https://www.figma.com/file/mKxRrAlvKmrR5DULHD2yah/Custom-metadata-Ingestion-Infrastructure
![Custom metadata Ingestion Infrastructure.png](Custom%20metadata%20Ingestion%20Infrastructure.png)