from unittest import TestCase

from cumulus_lambda_functions.granules_cnm_response_writer.cnm_result_writer import CnmResultWriter


class TestCnmResultWriter(TestCase):
    def test_01(self):
        sample_msg = {
            "collection": "URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:SNDR-SNPP_ATMS@L1B$OUTPUT",
            "identifier": "URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:SNDR-SNPP_ATMS@L1B$OUTPUT___2403261440:abcd.1234.efgh.test_file05",
            "submissionTime": "2024-05-01T13:35:23.796366",
            "provider": "unity",
            "version": "1.6.0",
            "product": {
                "dataVersion": "2403261440",
                "files": [
                    {
                        "type": "data",
                        "name": "abcd.1234.efgh.test_file05.nc",
                        "uri": "https://uds-distribution-placeholder/uds-sbx-cumulus-staging/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:SNDR-SNPP_ATMS@L1B$OUTPUT___2403261440/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:SNDR-SNPP_ATMS@L1B$OUTPUT___2403261440:abcd.1234.efgh.test_file05/abcd.1234.efgh.test_file05.nc",
                        "checksumType": "md5",
                        "checksum": "unknown",
                        "size": -1
                    },
                    {
                        "type": "metadata",
                        "name": "abcd.1234.efgh.test_file05.nc.cas",
                        "uri": "https://uds-distribution-placeholder/uds-sbx-cumulus-staging/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:SNDR-SNPP_ATMS@L1B$OUTPUT___2403261440/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:SNDR-SNPP_ATMS@L1B$OUTPUT___2403261440:abcd.1234.efgh.test_file05/abcd.1234.efgh.test_file05.nc.cas",
                        "checksumType": "md5",
                        "checksum": "unknown",
                        "size": -1
                    },
                    {
                        "type": "metadata",
                        "name": "URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:SNDR-SNPP_ATMS@L1B$OUTPUT___2403261440:abcd.1234.efgh.test_file05.cmr.xml",
                        "uri": "https://uds-distribution-placeholder/uds-sbx-cumulus-staging/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:SNDR-SNPP_ATMS@L1B$OUTPUT___2403261440/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:SNDR-SNPP_ATMS@L1B$OUTPUT___2403261440:abcd.1234.efgh.test_file05/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:SNDR-SNPP_ATMS@L1B$OUTPUT___2403261440:abcd.1234.efgh.test_file05.cmr.xml",
                        "checksumType": "md5",
                        "checksum": "63e0f7f9b76d56189267c854b67cd91b",
                        "size": 1858
                    }
                ],
                "name": "URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:SNDR-SNPP_ATMS@L1B$OUTPUT___2403261440:abcd.1234.efgh.test_file05"
            },
            "receivedTime": "2024-05-01T13:37:29.643Z",
            "response": {
                "status": "SUCCESS"
            },
            "processCompleteTime": "2024-05-01T13:38:01.676Z"
        }
        test = CnmResultWriter()
        test.cnm_response = sample_msg
        test.extract_s3_location()
        self.assertEqual('s3://uds-sbx-cumulus-staging/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:SNDR-SNPP_ATMS@L1B$OUTPUT___2403261440/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:SNDR-SNPP_ATMS@L1B$OUTPUT___2403261440:abcd.1234.efgh.test_file05/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:SNDR-SNPP_ATMS@L1B$OUTPUT___2403261440:abcd.1234.efgh.test_file05.2024-05-01T13:35:23.796366.cnm.json', test.s3_url)
        return
