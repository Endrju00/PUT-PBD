%DECLARE tripDataPath $input_dir3/results.tsv
%DECLARE taxiZonesPath $input_dir4/taxi_zone_lookup.csv
%declare outputPath $output_dir6

tripData = LOAD '$tripDataPath' USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t',
    'NO_MULTILINE', 'NOCHANGE')
    as (month:chararray,
        puLocationID:int,
        passengerCount:int);
DESCRIBE tripData;

taxiZones = LOAD '$taxiZonesPath' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',',
    'NO_MULTILINE', 'NOCHANGE')
    as (locationID:int,
        borough:chararray);
DESCRIBE taxiZones;

joined = JOIN tripData BY puLocationID, taxiZones BY locationID;
DESCRIBE joined;

grouped = GROUP joined by (month, borough);
DESCRIBE grouped;

passengerCount = FOREACH grouped GENERATE group.month as month, group.borough as borough, SUM(joined.passengerCount) as count;
DESCRIBE passengerCount;

groupedByMonth = GROUP passengerCount by month;
DESCRIBE groupedByMonth;

topResults = FOREACH groupedByMonth {
    sorted = ORDER passengerCount BY count DESC;
    top = LIMIT sorted 3;
    GENERATE group, FLATTEN(top);
}
DESCRIBE topResults;

finalResults = FOREACH topResults GENERATE top::month as month, top::borough as borough, top::count as passengerCount;
DESCRIBE finalResults;

STORE finalResults INTO '$outputPath' USING JsonStorage();