/*
 Analyze stock information from yahoo finance
 $head -2 yahooFinance.csv
 Date,Open,High,Low,Close,Volume,Adj Close
 2016-04-15,37.130001,37.150002,36.419998,36.509998,19016200,36.509998
*/

REGISTER '/user/acdepand/pig/piggybank.jar';
-- PiggyBank.storage.CSVLoader will automatically use 1st column as header
-- If no header is present, one can use PiggyBank.storage.CSVExcelStorage 
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;
 
stocks = LOAD '/user/acdepand/pig/pig_hw/yahooFinance.csv' USING CSVLoader AS 
        (date: chararray, open: float, high: float, low: float, close: float, volumn: float, adjClose: float);
stocks_count = FOREACH (GROUP stocks ALL) GENERATE COUNT(stocks); --5038
    
--Check the stocks with closing price higher than 40
closeGreaterThan40 = FILTER stocks BY (close > 40.0);
closeGreaterThan40_count = FOREACH (GROUP closeGreaterThan40 ALL) GENERATE COUNT(closeGreaterThan40); --1248

--Check the stocks with high price lower than 35 in year 2016
highLessThan35 = FILTER stocks BY (high < 35.0 AND date > '2015-12-31');
highLessThan35_count = FOREACH (GROUP highLessThan35 ALL) GENERATE COUNT(highLessThan35); --53

--Find stock whose high and low difference is greater than 1 or high greater than 50.
highLowDiffGreaterThan1orHighGreaterThan50 = FILTER stocks BY (high-low > 1 OR high > 50);
highLowDiffGreaterThan1orHighGreaterThan50_count = FOREACH (GROUP highLowDiffGreaterThan1orHighGreaterThan50 
    ALL) GENERATE COUNT(highLowDiffGreaterThan1orHighGreaterThan50); --2105
    
--Find top 10 stocks with highest close prices.
highestClose = ORDER stocks BY close DESC;
highestCloseTop10 = LIMIT highestClose 10;
dump highestCloseTop10;

--Split stocks into three groups, group1 if close < 30, group2 if close > 40, group3 otherwise.
SPLIT stocks INTO group1 IF close < 30, group2 IF close > 40, group3 otherwise;


-- Other useful operations
-- filter 2015 stocks with Regular Expression
stocks2015 = FILTER stocks BY $0 MATCHES '.*2015.*';
--$0: 1st column

--sampling data
sampleStock = SAMPLE stocks 0.01;
store sampleStock into '/user/acdepand/pig/sampleStock';
