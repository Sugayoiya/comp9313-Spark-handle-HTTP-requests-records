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
