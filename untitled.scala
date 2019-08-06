import scala.xml.XML
import org.json.JSONObject
import org.json.{XML => JXML}
import java.io._
import scalaj.http._
import org.json4s._
//import org.json4s.native.JsonMethods._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.{Set => mSet}
import scala.collection.mutable.{Map => mMap}
import scala.collection.mutable.ListBuffer


// get list of files 
val tem = "/home/moko/Documents/comp9313/ass3/cases_test"
val files = (new File(tem)).listFiles.filter { f => f.isFile && (f.getName.endsWith(".xml")) }.map(_.getAbsolutePath)
implicit val formats = DefaultFormats

//val mapping_response=Http("http://localhost:9200/testid/cases/_mapping?pretty").postData("""{"cases":{"properties":{"id":{"type":"text"},"name":{"type":"text"},"url":{"type":"text"},"catchphrase":{"type":"text"},"sentence":{"type":"text"},"person":{"type":"text"},"location":{"type":"text"},"organization":{"type":"text"}}}}""").method("PUT").header("Content-Type", "application/json").option(HttpOptions.connTimeout(10000)).option(HttpOptions.readTimeout(10000)).asString

for(x<-files){
	println(x) // print out all files
	val xml = scala.xml.XML.loadFile(x)
	val filename = x.split("\\/").last.split("\\.")(0)
	//println(filename)
	val name = (xml \ "name").text
	val url = (xml \ "AustLII").text
	val catchphrase = new ListBuffer[String]()
	for(x<-(xml \ "catchphrases" \ "catchphrase")){
		catchphrase += x.text
	}
	val sentence = new ListBuffer[String]()
	for(x<-(xml \ "sentences" \ "sentence")){
		sentence += x.text
	}

	// store the enriched entities 
	var organization = mSet[String]()
	var location = mSet[String]()
	var person = mSet[String]()

	for(x<-sentence){
		val nlp = Http("http://localhost:9000/").params("annotators"->"entitymentions","outputFormat"->"json","ner.model"->"edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz","ner.useSUTime"->"false").postData(x.toString).timeout(connTimeoutMs = 1000000, readTimeoutMs = 5000000).asString.body
		val nlp_json = parse(nlp)
		// println(nlp_json)
		println("-----",x)
		val entitymention = (nlp_json \ "sentences"\ "entitymentions").asInstanceOf[JArray].arr(0)
		//println("&&&&&&&&",entitymention.arr.length)
		val nnn = (entitymention \"ner").asInstanceOf[JArray]
		val ttt = (entitymention \"text").asInstanceOf[JArray]

		println("nnn,ttt")
		for(y<-0 until nnn.arr.length){
			if ((nnn.arr(y) \ "ner").extract[String] == "LOCATION") {
				println("loc")
				location += (ttt.arr(y) \ "text").extract[String]
			}else if ((nnn.arr(y) \ "ner").extract[String] == "PERSON") {
				println("per")
				person += (ttt.arr(y) \ "text").extract[String]
			}else if ((nnn.arr(y) \ "ner").extract[String] == "ORGANIZATION") {
				println("org")
				organization += (ttt.arr(y) \ "text").extract[String]
			}
		}
	}

	new PrintWriter("nlp.txt") { write(nlp); close }
	println(organization)
	println(location)
	println(person)

	val xmlJSONObj:JSONObject = JXML.toJSONObject(xml.toString)
	xmlJSONObj.getJSONObject("case").put("location",location.toArray)
	xmlJSONObj.getJSONObject("case").put("person",person.toArray)
	xmlJSONObj.getJSONObject("case").put("organization",organization.toArray)
	xmlJSONObj.getJSONObject("case").put("sentences",sentence.toArray)
	xmlJSONObj.getJSONObject("case").put("catchphrases",catchphrase.toArray)

	val contructJson = ""

	new PrintWriter("xmlJSONObj.txt") { write(xmlJSONObj.toString); close }

	val saveInElasticSearch = Http("http://localhost:9200/testid/cases/"+filename+"?pretty").postData(xmlJSONObj.toString).method("PUT").header("Content-Type", "application/json").timeout(connTimeoutMs = 1000000, readTimeoutMs = 5000000).asString
}


// val aaa = "The transfers: &#8226; have been properly stamped; &#8226; are signed by a member of Tower as transferor and Pendant Software as transferee; &#8226; are in respect of shares in Tower of which the transferring member is registered as the holder; and &#8226; relate to shares in respect of which rule 120 of Tower's Constitution either does not apply or has been satisfied; 2. to do so would not involve a breach of duty on the part of Directors and it is not otherwise unlawful; and 3. no higher offer is made for Tower Software Engineering Pty Limited's shares during the Offer Period (as extended under the Corporations Act )."
// new PrintWriter("nlp1.txt") { write(nlp.toString); close }

// val bbb = "16 Mr Frost in his capacity as a director of Pendant Software, was concerned that;"

val ccc = "It arises in the following circumstances."

val ddd = "(NOTICE OF MOTION) EDMONDS J: 1 Pursuant to a notice of motion filed on 25 November 2005 ('the Notice of Motion') the first respondent/second cross-respondent, Diageo Australia Limited ('Diageo'), moved the Court for orders (1) that pursuant to O 29 r 2(a) of the Federal Court Rules , all issues under the Further Amended Cross-Claim filed on 12 September 2005 ('the Cross-Claim') be determined separately from, and prior to, the issues in the main proceeding; and (2) that the main proceeding be stayed until determination of the Cross-Claim, subject to any conditions imposed by the Court."