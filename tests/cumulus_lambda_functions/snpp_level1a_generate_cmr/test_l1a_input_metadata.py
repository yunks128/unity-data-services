import unittest
import xmltodict

from cumulus_lambda_functions.snpp_level1a_generate_cmr.l1a_input_metadata import L1AInputMetadata


class TestL1aInputMetadata(unittest.TestCase):
    def test_01(self):
        input_str = '''<?xml version="1.0" encoding="UTF-8" ?>
<cas:metadata xmlns:cas="http://oodt.jpl.nasa.gov/1.0/cas">
    <keyval type="scalar">
        <key>AggregateDir</key>
        <val>snppatmsl1a</val>
    </keyval>
    <keyval type="vector">
        <key>AutomaticQualityFlag</key>
        <val>Passed</val>
    </keyval>
    <keyval type="vector">
        <key>BuildId</key>
        <val>v01.43.00</val>
    </keyval>
    <keyval type="vector">
        <key>CollectionLabel</key>
        <val>L1AMw_nominal2</val>
    </keyval>
    <keyval type="scalar">
        <key>DataGroup</key>
        <val>sndr</val>
    </keyval>
    <keyval type="scalar">
        <key>EndDateTime</key>
        <val>2016-01-14T10:00:00.000Z</val>
    </keyval>
    <keyval type="scalar">
        <key>EndTAI93</key>
        <val>726919209.000</val>
    </keyval>
    <keyval type="scalar">
        <key>FileFormat</key>
        <val>nc4</val>
    </keyval>
    <keyval type="scalar">
        <key>FileLocation</key>
        <val>/pge/out</val>
    </keyval>
    <keyval type="scalar">
        <key>Filename</key>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.01.nc</val>
    </keyval>
    <keyval type="vector">
        <key>GranuleNumber</key>
        <val>100</val>
    </keyval>
    <keyval type="scalar">
        <key>JobId</key>
        <val>f163835c-9945-472f-bee2-2bc12673569f</val>
    </keyval>
    <keyval type="scalar">
        <key>ModelId</key>
        <val>urn:npp:SnppAtmsL1a</val>
    </keyval>
    <keyval type="scalar">
        <key>NominalDate</key>
        <val>2016-01-14</val>
    </keyval>
    <keyval type="vector">
        <key>ProductName</key>
        <val>SNDR.SNPP.ATMS.20160114T0954.m06.g100.L1A.L1AMw_nominal2.v03_15_00.D.201214135000.nc</val>
    </keyval>
    <keyval type="scalar">
        <key>ProductType</key>
        <val>SNDR_SNPP_ATMS_L1A</val>
    </keyval>
    <keyval type="scalar">
        <key>ProductionDateTime</key>
        <val>2020-12-14T13:50:00.000Z</val>
    </keyval>
    <keyval type="vector">
        <key>ProductionLocation</key>
        <val>Sounder SIPS: JPL/Caltech (Dev)</val>
    </keyval>
    <keyval type="vector">
        <key>ProductionLocationCode</key>
        <val>D</val>
    </keyval>
    <keyval type="scalar">
        <key>RequestId</key>
        <val>1215</val>
    </keyval>
    <keyval type="scalar">
        <key>StartDateTime</key>
        <val>2016-01-14T09:54:00.000Z</val>
    </keyval>
    <keyval type="scalar">
        <key>StartTAI93</key>
        <val>726918849.000</val>
    </keyval>
    <keyval type="scalar">
        <key>TaskId</key>
        <val>8c3ae101-8f7c-46c8-b5c6-63e7b6d3c8cd</val>
    </keyval>
</cas:metadata>
'''
        l1a = L1AInputMetadata(xmltodict.parse(input_str)).load()
        self.assertEqual(l1a.beginning_dt, '2016-01-14T09:54:00.000Z', 'wrong beginning_dt')
        self.assertEqual(l1a.ending_dt, '2016-01-14T10:00:00.000Z', 'wrong ending_dt')
        self.assertEqual(l1a.prod_dt, '2020-12-14T13:50:00.000Z', 'wrong prod_dt')
        self.assertEqual(l1a.prod_name, 'SNDR.SNPP.ATMS.20160114T0954.m06.g100.L1A.L1AMw_nominal2.v03_15_00.D.201214135000.nc', 'wrong prod_name')
        return

    def test_02(self):
            input_str = '''<?xml version="1.0" encoding="UTF-8" ?>
<cas:metadata xmlns:cas="http://oodt.jpl.nasa.gov/1.0/cas">
    <keyval type="scalar">
        <key>AggregateDir</key>
        <val>snppatmsl1b</val>
    </keyval>
    <keyval type="vector">
        <key>AutomaticQualityFlag</key>
        <val>Passed</val>
    </keyval>
    <keyval type="vector">
        <key>BuildId</key>
        <val>v01.43.00</val>
    </keyval>
    <keyval type="vector">
        <key>CollectionLabel</key>
        <val>L1BMw_nominal</val>
    </keyval>
    <keyval type="scalar">
        <key>DataGroup</key>
        <val>sndr</val>
    </keyval>
    <keyval type="scalar">
        <key>EndDateTime</key>
        <val>2016-01-14T11:48:00.000Z</val>
    </keyval>
    <keyval type="scalar">
        <key>EndTAI93</key>
        <val>726925689.000</val>
    </keyval>
    <keyval type="scalar">
        <key>FileFormat</key>
        <val>nc4</val>
    </keyval>
    <keyval type="scalar">
        <key>FileLocation</key>
        <val>/ddlyVp</val>
    </keyval>
    <keyval type="scalar">
        <key>Filename</key>
        <val>test_file19.nc</val>
    </keyval>
    <keyval type="vector">
        <key>GranuleNumber</key>
        <val>118</val>
    </keyval>
    <keyval type="vector">
        <key>InputFiles</key>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.01.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.02.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.03.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.04.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.05.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.06.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.07.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.08.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.09.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.10.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.11.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.12.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.13.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.14.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.15.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.16.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.17.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.18.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.19.nc</val>
        <val>SNDR.SNPP.ATMS.L1A.nominal2.20.nc</val>
        <val>SNDR.SNPP.L1bMw.apf.171115000000.xml</val>
        <val>SNDR.SNPP.L1bMw.template.201217000000.nc</val>
    </keyval>
    <keyval type="scalar">
        <key>JobId</key>
        <val>f163835c-9945-472f-bee2-2bc12673569f</val>
    </keyval>
    <keyval type="scalar">
        <key>ModelId</key>
        <val>urn:npp:SnppAtmsL1b</val>
    </keyval>
    <keyval type="scalar">
        <key>NominalDate</key>
        <val>2016-01-14</val>
    </keyval>
    <keyval type="vector">
        <key>ProductName</key>
        <val>SNDR.SNPP.ATMS.20160114T1142.m06.g118.L1B.L1BMw_nominal.v03_07.D.150520120000.nc</val>
    </keyval>
    <keyval type="scalar">
        <key>ProductType</key>
        <val>SNDR_SNPP_ATMS_L1B</val>
    </keyval>
    <keyval type="scalar">
        <key>ProductionDateTime</key>
        <val>2015-05-20T12:00:00.000Z</val>
    </keyval>
    <keyval type="vector">
        <key>ProductionLocation</key>
        <val>Sounder SIPS: JPL/Caltech (Dev)</val>
    </keyval>
    <keyval type="vector">
        <key>ProductionLocationCode</key>
        <val>D</val>
    </keyval>
    <keyval type="scalar">
        <key>RequestId</key>
        <val>12566</val>
    </keyval>
    <keyval type="scalar">
        <key>StartDateTime</key>
        <val>2016-01-14T11:42:00.000Z</val>
    </keyval>
    <keyval type="scalar">
        <key>StartTAI93</key>
        <val>726925329.000</val>
    </keyval>
    <keyval type="scalar">
        <key>TaskId</key>
        <val>8c3ae101-8f7c-46c8-b5c6-63e7b6d3c8cd</val>
    </keyval>
</cas:metadata>
    '''
            l1a = L1AInputMetadata(xmltodict.parse(input_str)).load()
            self.assertEqual(l1a.beginning_dt, '2016-01-14T11:42:00.000Z', 'wrong beginning_dt')
            self.assertEqual(l1a.ending_dt, '2016-01-14T11:48:00.000Z', 'wrong ending_dt')
            self.assertEqual(l1a.prod_dt, '2015-05-20T12:00:00.000Z', 'wrong prod_dt')
            self.assertEqual(l1a.prod_name,
                             'SNDR.SNPP.ATMS.20160114T1142.m06.g118.L1B.L1BMw_nominal.v03_07.D.150520120000.nc',
                             'wrong prod_name')
            return
