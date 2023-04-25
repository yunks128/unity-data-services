import unittest

import xmltodict

from cumulus_lambda_functions.lib.metadata_extraction.echo_metadata import EchoMetadata
from cumulus_lambda_functions.metadata_s4pa_generate_cmr.pds_metadata import PdsMetadata


class TestEchoMetadata(unittest.TestCase):
    def test_01(self):
        input_str = '''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="http://snppl0.gesdisc.eosdis.nasa.gov/data/S4paGran2HTML.xsl"?>
<S4PAGranuleMetaDataFile>
  <CollectionMetaData>
    <ShortName>ATMS_SCIENCE_Group</ShortName>
    <VersionID>1</VersionID>
  </CollectionMetaData>
  <DataGranule>
    <GranuleID>P1570515ATMSSCIENCEAXT11349120000000.PDS</GranuleID>
    <CheckSum>
      <CheckSumType>CRC32</CheckSumType>
      <CheckSumValue>3146348796</CheckSumValue>
    </CheckSum>
    <ProductionDateTime>2011-12-15T12:00:00.000000Z</ProductionDateTime>
    <SizeBytesDataGranule>18085344</SizeBytesDataGranule>
    <InsertDateTime>2016-12-19 19:55:36</InsertDateTime>
    <Granulits>
      <Granulit>
        <GranulitID>1</GranulitID>
        <FileName>P1570515ATMSSCIENCEAXT11349120000001.PDS</FileName>
        <FileSize>18084600</FileSize>
        <CheckSum>
          <CheckSumType>CRC32</CheckSumType>
          <CheckSumValue>2339567270</CheckSumValue>
        </CheckSum>
      </Granulit>
      <Granulit>
        <GranulitID>0</GranulitID>
        <FileName>P1570515ATMSSCIENCEAXT11349120000000.PDS</FileName>
        <FileSize>744</FileSize>
        <CheckSum>
          <CheckSumType>CRC32</CheckSumType>
          <CheckSumValue>3146348796</CheckSumValue>
        </CheckSum>
      </Granulit>
    </Granulits>
  </DataGranule>
  <RangeDateTime>
    <RangeEndingDate>2011-12-15</RangeEndingDate>
    <RangeEndingTime>13:59:59.991039</RangeEndingTime>
    <RangeBeginningDate>2011-12-15</RangeBeginningDate>
    <RangeBeginningTime>12:00:00.009054</RangeBeginningTime>
  </RangeDateTime>
  <Platform>
    <PlatformShortName>SNPP</PlatformShortName>
    <Instrument>
      <InstrumentShortName>ATMS</InstrumentShortName>
      <Sensor>
        <SensorShortName>ATMS Channel 1</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 2</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 3</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 4</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 5</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 6</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 7</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 8</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 9</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 10</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 11</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 12</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 13</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 14</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 15</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 16</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 17</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 18</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 19</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 20</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 21</SensorShortName>
      </Sensor>
      <Sensor>
        <SensorShortName>ATMS Channel 22</SensorShortName>
      </Sensor>
    </Instrument>
  </Platform>
  <EDOSDataType>ATMS_SCIENCE_Group</EDOSDataType>
  <ProducersMetaData>GROUP = INVENTORYMETADATA
GROUPTYPE = MASTERGROUP
    GROUP = CollectionDescriptionClass
        OBJECT = ShortName
            Value = "ATMS_SCIENCE_Group"
            TYPE = "STRING"
            NUM_VAL = 1
        END_OBJECT = ShortName
        OBJECT = VersionID
            Value = 1
            TYPE = "INTEGER"
            NUM_VAL = 1
        END_OBJECT = VersionID
    END_GROUP = CollectionDescriptionClass
    GROUP = ECSDataGranule
        OBJECT = SizeMBECSDataGranule
            Value = 18.085344
            TYPE = "DOUBLE"
            NUM_VAL = 1
        END_OBJECT = SizeMBECSDataGranule
        OBJECT = ProductionDateTime
            Value = "2011-12-15T12:00:00.000000Z"
            TYPE = "TIME"
            NUM_VAL = 1
        END_OBJECT = ProductionDateTime
    END_GROUP = ECSDataGranule
    GROUP = PGEVersionClass
    END_GROUP = PGEVersionClass
    GROUP = RangeDateTime
        OBJECT = RangeEndingTime
            Value = "13:59:59.991039"
            TYPE = "STRING"
            NUM_VAL = 1
        END_OBJECT = RangeEndingTime
        OBJECT = RangeEndingDate
            Value = "2011-12-15"
            TYPE = "DATE"
            NUM_VAL = 1
        END_OBJECT = RangeEndingDate
        OBJECT = RangeBeginningTime
            Value = "12:00:00.009054"
            TYPE = "STRING"
            NUM_VAL = 1
        END_OBJECT = RangeBeginningTime
        OBJECT = RangeBeginningDate
            Value = "2011-12-15"
            TYPE = "DATE"
            NUM_VAL = 1
        END_OBJECT = RangeBeginningDate
    END_GROUP = RangeDateTime
    GROUP = SpatialDomainContainer
        GROUP = HorizontalSpatialDomainContainer
            GROUP = ZoneIdentifierClass
            END_GROUP = ZoneIdentifierClass
        END_GROUP = HorizontalSpatialDomainContainer
    END_GROUP = SpatialDomainContainer
    GROUP = AdditionalAttributes
        OBJECT = AdditionalAttributesContainer
            CLASS = "1"
            OBJECT = AdditionalAttributeName
                CLASS = "1"
                Value = "PDS_ID"
                TYPE = "STRING"
                NUM_VAL = 1
            END_OBJECT = AdditionalAttributeName
            GROUP = InformationContent
                CLASS = "1"
                OBJECT = ParameterValue
                    CLASS = "1"
                    Value = ("P1570515ATMSSCIENCEAXT11349120000000")
                    TYPE = "STRING"
                    NUM_VAL = 1
                END_OBJECT = ParameterValue
            END_GROUP = InformationContent
        END_OBJECT = AdditionalAttributesContainer
    END_GROUP = AdditionalAttributes
    GROUP = AdditionalAttributes
        OBJECT = AdditionalAttributesContainer
            CLASS = "2"
            OBJECT = AdditionalAttributeName
                CLASS = "1"
                Value = "EDOSDataType"
                TYPE = "STRING"
                NUM_VAL = 1
            END_OBJECT = AdditionalAttributeName
            GROUP = InformationContent
                CLASS = "1"
                OBJECT = ParameterValue
                    CLASS = "1"
                    Value = ("ATMS_SCIENCE_Group")
                    TYPE = "STRING"
                    NUM_VAL = 1
                END_OBJECT = ParameterValue
            END_GROUP = InformationContent
        END_OBJECT = AdditionalAttributesContainer
    END_GROUP = AdditionalAttributes
    GROUP = DataFiles
        
        OBJECT = DataFileContainer
            CLASS = "1"
            OBJECT = DistributedFileName
                CLASS = "1"
                Value = "P1570515ATMSSCIENCEAXT11349120000001.PDS"
                TYPE = "STRING"
                NUM_VAL = 1
            END_OBJECT = DistributedFileName
            OBJECT = FileSize
                CLASS = "1"
                Value = 18084600
                TYPE = "INTEGER"
                NUM_VAL = 1
            END_OBJECT = FileSize
            OBJECT = ChecksumType
                CLASS = "1"
                Value = "CKSUM"
                TYPE = "STRING"
                NUM_VAL = 1
            END_OBJECT = ChecksumType
            OBJECT = Checksum
                CLASS = "1"
                Value = "2339567270"
                TYPE = "STRING"
                NUM_VAL = 1
            END_OBJECT = Checksum
            OBJECT = ChecksumOrigin
                CLASS = "1"
                Value = "DataProvider"
                TYPE = "STRING"
                NUM_VAL = 1
            END_OBJECT = ChecksumOrigin
        END_OBJECT = DataFileContainer
        
        OBJECT = DataFileContainer
            CLASS = "2"
            OBJECT = DistributedFileName
                CLASS = "2"
                Value = "P1570515ATMSSCIENCEAXT11349120000000.PDS"
                TYPE = "STRING"
                NUM_VAL = 1
            END_OBJECT = DistributedFileName
            OBJECT = FileSize
                CLASS = "2"
                Value = 744
                TYPE = "INTEGER"
                NUM_VAL = 1
            END_OBJECT = FileSize
            OBJECT = ChecksumType
                CLASS = "2"
                Value = "CKSUM"
                TYPE = "STRING"
                NUM_VAL = 1
            END_OBJECT = ChecksumType
            OBJECT = Checksum
                CLASS = "2"
                Value = "3146348796"
                TYPE = "STRING"
                NUM_VAL = 1
            END_OBJECT = Checksum
            OBJECT = ChecksumOrigin
                CLASS = "2"
                Value = "DataProvider"
                TYPE = "STRING"
                NUM_VAL = 1
            END_OBJECT = ChecksumOrigin
        END_OBJECT = DataFileContainer
        
    END_GROUP = DataFiles
END_GROUP = INVENTORYMETADATA
END</ProducersMetaData>
</S4PAGranuleMetaDataFile>        '''
        granule_metadata_props = PdsMetadata(xmltodict.parse(input_str)).load()
        self.assertEqual(granule_metadata_props.granule_id, 'P1570515ATMSSCIENCEAXT11349120000000.PDS', 'wrong granule_id')

        echo_metadata = EchoMetadata(granule_metadata_props).load().echo_metadata
        self.assertTrue('Granule' in echo_metadata, 'missing Granule')
        echo_granule = echo_metadata['Granule']
        self.assertTrue('InsertTime' in echo_granule, 'missing InsertTime')
        self.assertTrue('GranuleUR' in echo_granule, 'missing InsertTime')
        self.assertEqual(echo_granule['GranuleUR'], 'P1570515ATMSSCIENCEAXT11349120000000.PDS', 'wrong GranuleUR')
        self.assertEqual(echo_granule['InsertTime'], '2016-12-19 19:55:36', 'wrong InsertTime')
        return
