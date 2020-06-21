// Databricks notebook source
val mov_info = sc.textFile("/FileStore/tables/movie_metadata-ab497.tsv")
val plots = sc.textFile("/FileStore/tables/plot_summaries.txt")
val stop_words = sc.textFile("/FileStore/tables/stopwords.txt")
val mov_cnt = mov_info.count()

// COMMAND ----------

val keyid = mov_info.map(x => (x.split("\t")(0), x.split("\t")(2)))

// COMMAND ----------

val stop_words_1 = stop_words.flatMap(x => x.split(",")).collect().toSet
val pun_filter = plots.map(x => x.replaceAll("""[\p{Punct}]""", " ")).map(x => x.toLowerCase.split("""\s+"""))

// COMMAND ----------

val stop_words_filtered = pun_filter.map((x => x.map(y => y).filter(z => stop_words_1.contains(z) == false)))
stop_words_filtered.take(30)


// COMMAND ----------

val given_query = spark.read.csv("/FileStore/tables/input.txt")
val search_terms = given_query.head(1)
val search_terms_array = search_terms.map(_.getString(0))
val search_t = search_terms_array(0).toLowerCase().split(" ")
val search_terms_length = search_t.length

// COMMAND ----------

if (search_terms_length > 1){
def termff(y: String) = stop_words_filtered.map(x => (x(0), x.count(_.contains(y)).toDouble/x.size)).filter(x=>x._2!=0.0)
val term_freq = search_t.map(y => termff(y).collect().toMap)

def docff(word: String) = mov_info.flatMap(doc => doc.split("\n").filter(line => line.contains(word))).map(doc => ("line", 1)).reduceByKey(_ + _).collect()(0)._2
val doc_freq = search_t.map(x => docff(x))
val inv_doc_freq = doc_freq.map(y => (1+ math.log(mov_cnt/y)))

def tifunc(x: Int) = term_freq(x).map(a=>(a._1,a._2*inv_doc_freq(x))).toMap
val tifunc1 = term_freq.zipWithIndex.map{ case (a, b) =>tifunc(b) }

val searchtfunc =  search_t.map(y => search_t.count(_.contains(y)).toDouble/search_t.size)
val searchtifunc = searchtfunc.zipWithIndex.map{case (a, b) => a * inv_doc_freq(b)}
val search_term = math.sqrt(searchtifunc.reduce((x,y) => x *x + y *y))
val mov = tifunc1.flatMap(x => x.map(y=>y._1)).toList.distinct.toArray

def mag_func(x:String)= search_t.zipWithIndex.map{case (a, b) => (tifunc(b).get(x).getOrElse(0.0).asInstanceOf[Double]).toDouble }.reduce((x,y)=>x*x+y*y)
val rec = mov.map(x => (x, math.sqrt(mag_func(x)))).toMap

def dp_func(x:String)= search_t.zipWithIndex.map{case (e, i) => (searchtifunc(i) * tifunc1(i).get(x).getOrElse(0.0).asInstanceOf[Double]).toDouble}.reduce((x,y)=>x+y)

val dp = mov.map(x => (x, dp_func(x))).toMap
val cos_value = mov.map( x=> (x, dp.get(x).getOrElse(0.0).asInstanceOf[Double] / (rec.get(x).getOrElse(0.0).asInstanceOf[Double] * search_term)))
val cos= sc.parallelize(cos_value)
val des_mov = keyid.join(cos).map(x=>(x._2._1,x._2._2)).sortBy(_._2).map(_._1).take(10)

val final_order = sc.parallelize(des_mov)
final_order.collect()
}

// COMMAND ----------

if(search_terms_length==1)
{

  val term_freq = stop_words_filtered.map(x => (x(0), x.count(_.contains(search_t)).toDouble / x.size)).filter(y => y._2 != 0.0)
  val doc_freq = mov_info.flatMap(x => x.split("\n").filter(y => y.contains(search_t))).map(x => ("y", 1)).reduceByKey(_ + _).collect()(0)._2
  val inv_doc_freq = 1 + math.log(mov_cnt / doc_freq)
  val tifunc = term_freq.map(factor => (factor._1, factor._2 * inv_doc_freq))
  val des_mov = keyid.join(tifunc).map(factor => (factor._2._1, factor._2._2)).sortBy(-_._2).map(_._1).take(10)
  val final_order = sc.parallelize(des_mov)
  final_order.collect()
}
