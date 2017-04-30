--Wordcount of PIG

book = load 'path/to/book'; 
--book:(line1)(line2),...

wordlist = FOREACH book GENERATE flatten(TOKENIZE((chararray)$0)) as word;
--(chararray) - cast
--$0 - 1st column
--TOKENIZE: default space.
--without flattern: ({(line1word1),(word2),(word3)})({(line2word1),(word2),(word3),(word4))
--after flattern:(word1),(word2),(word3),(word4),(word5),(word6),(word7))

wordgroup = GROUP wordlist by word;
wordcount = FOREACH wordgroup GENERATE word, COUNT(wordlist) as total;

wordOrder = ORDER wordcount BY total DESC;

store wordOrder into 'new/folder/to/dest';
