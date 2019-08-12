# comp9313-scala-assignment

read the assignment2.pdf

change those data to Base URL, minimum payload, maximum payload, mean payload, variance of payload
`http://subdom0001.example.com,/endpoint0001,POST,3B
http://subdom0002.example.com,/endpoint0002,GET,431MB
http://subdom0003.example.com,/endpoint0003,POST,231KB
http://subdom0002.example.com,/endpoint0002,GET,29MB
http://subdom0001.example.com,/endpoint0001,POST,238B
http://subdom0002.example.com,/endpoint0001,GET,32MB
http://subdom0003.example.com,/endpoint0003,GET,21KB
`
to

`
http://subdom0001.example.com,3B,238B,120B,13806B
http://subdom0002.example.com,30408704B,451936256B,171966464B,39193191483703296B
http://subdom0003.example.com,21504B,236544B,129024B,11560550400B
`


resolve the problem described in pdf

sample input text file is "sample_input.txt"
sample output text file is "sample_output.txt"
the real scala output is in "output" directory

change the first two lines "inputFilePath" and "outputDirPath" for other input file and output directory. 
