# COMP9313 Big Data Management

### student name: Hang Zhang

### student ID: z5153042



#### solution explanation:



* Define two variables as input file path and output file directory in the very beginning. Because this scala program file runs in spark-shell, there is a default $SparkContext$ to read file.
* Create a function to transform the lines read from files to be like "http://subdom000X.example.com,Y". the unit of Y is **bytes**. So the number followed by **KB** should times $1024$ and the one followed by **MB **should times ${1024^2}$  .
* We can use $math.max$ and $math.min$ in the $reduceByKey $ to calculate the maximum and minimum by key, then join them together for later use.
* Then group all the bytes which are the same key and get the group size in order to calculate the mean.
* Join the sum of values of each key with the size, calculate the mean.
* Join the normalized line in the step 2 (http://subdom000X.example.com,Y)  with mean and remap them following the rule  $(Y - mean)^2$ by key we can get the squared value.
* The squared value divided by the relative size we can get the variance by key.
* Map the value we want together in the output RDD.
* Create a function to transform  Double numbers ( e.g : $3.9193191483703296E16$) to String with the unit "$B$" ( e.g :$39193191483703296B$).
* Get the final output by remap them using $mkString(",")$ to remove the blankets and $saveAsTextFile(outputDirPath)$ to save it in a local txt file.

