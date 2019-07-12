// Use the named values (val) below whenever your need to
// read/write inputs and outputs in your program. 
val inputFilePath  = "/home/moko/Documents/comp9313/ass2/sample_input.txt"
val outputDirPath = "/home/moko/Documents/comp9313/ass2/output"


// Write your solution here

// read the file
val line=sc.textFile(inputFilePath,1)

// function to normalize payload to Double(bytes)
def transformBytes(a:String):Double = {
     var sum:Double=a.split("[a-zA-Z]+")(0).toDouble
     var unit:String=a.split("\\d+")(1)
     if(unit=="KB"){sum=sum*1024}
     else if (unit=="MB"){sum=sum*1024*1024}
     return sum
     }

// normalized lines
val words = line.map(x=>(x.split(",")(0),transformBytes(x.split(",")(3))))


/* sample of words
(http://subdom0001.example.com,3)
(http://subdom0002.example.com,451936256)
(http://subdom0003.example.com,236544)
(http://subdom0002.example.com,30408704)
(http://subdom0001.example.com,238)
(http://subdom0002.example.com,33554432)
(http://subdom0003.example.com,21504)
*/

// calculate the maximum and minimum by key
val max = words.reduceByKey(math.max(_,_))
val min = words.reduceByKey(math.min(_,_))

// join two dicts(max&min) by key
val min_max = min.join(max) 

/* sample of min_max
min_max.foreach(println)
(http://subdom0003.example.com,(21504,236544))
(http://subdom0001.example.com,(3,238))
(http://subdom0002.example.com,(30408704,451936256))
*/

// get the values' size by key
val size = words.groupByKey().map(k=>(k._1,k._2.iterator.size)) 

/* sample of size
size.foreach(println)
(http://subdom0003.example.com,2)
(http://subdom0001.example.com,2)
(http://subdom0002.example.com,3)
*/

// form a dict like {key,value=>(sum,size)} in order to calculate mean
val sum_size = words.reduceByKey(_+_).join(size)

/* sample of sum_size
sum_size.foreach(println)
(http://subdom0003.example.com,(258048,2))
(http://subdom0001.example.com,(241,2))
(http://subdom0002.example.com,(515899392,3))
*/

// mean {key,value=>(sum/size)}
val mean = sum_size.map(x=>(x._1,x._2._1/x._2._2)) 
  
/* sample of mean
mean.foreach(println)
(http://subdom0003.example.com,129024)
(http://subdom0001.example.com,120)
(http://subdom0002.example.com,171966464)
*/

/*
val t = words.join(mean)
val tt = t.map(x=>(x._1,x._2._1-x._2._2))
val ttt = tt.map(x=>(x._1,x._2^2))  // get the square
*/

// get the square: (each payload - relative mean)^2
val t= words.join(mean).map(x=>(x._1,x._2._1-x._2._2)).map(x=>(x._1,x._2*x._2)) 

// variance: square/size
val variance = t.reduceByKey(_+_).join(size).map(x=>(x._1,x._2._1/x._2._2)) 

// output remaped as {key,min,max,mean(double),variance(double)}
val output = min_max.join(mean).join(variance).map(x=>(x._1,x._2._1._1._1,x._2._1._1._2,x._2._1._2,x._2._2))

/* sample of output
output.foreach(println)
(http://subdom0003.example.com,21504,236544,129024.0,1.15605504E10)
(http://subdom0001.example.com,3,238,120.5,13806.25)
(http://subdom0002.example.com,30408704,451936256,1.71966464E8,3.9193191483703296E16)
*/

// function to transform the Double numbers to String
def trans(a:Double):String = {
   val temp = a.toString.split("E")
   var out:String = ""
   if( temp.size == 2){
      val num = temp(0).split("\\.")
      if( num.size == 2){
         if (num(1).size < temp(1).toInt){
            out = out +num(0)+num(1)
            val i = temp(1).toInt - num(1).size
            val j:Int = 0
            for ( j <- 1 to i){
               out = out + "0"
            }
            out = out + "B"
            return out
         } else {
            out = out +num(0)+num(1).take(temp(1).toInt)
            out = out +"B"
            return out
         }
      } else {
          val j:Int = 0
          for ( j <- 1 to temp(1).toInt){
             out = out + "0"
          }
          out = out + "B"
          return out
      }
   } else {
      val num = temp(0).split("\\.")
      out = out + num(0) +"B"
      return out
   }
   return out
}

// final output
val finalout = output.map(x=>(x._1,trans(x._2),trans(x._3),trans(x._4),trans(x._5))) 

/* sample of final output
finalout.foreach(println)
(http://subdom0003.example.com,21504B,236544B,129024B,11560550400B)
(http://subdom0001.example.com,3B,238B,120B,13806B)
(http://subdom0002.example.com,30408704B,451936256B,171966464B,39193191483703296B)
*/

// output to file sorted by key(optional)
finalout.sortBy{case(a,b,c,d,e)=>a}.map{case(a,b,c,d,e)=>Array(a,b,c,d,e).mkString(",")}.saveAsTextFile(outputDirPath)




