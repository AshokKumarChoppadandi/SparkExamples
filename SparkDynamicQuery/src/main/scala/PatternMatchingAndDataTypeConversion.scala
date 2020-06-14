import java.time.LocalDate
import java.time.format.DateTimeFormatter

object PatternMatchingAndDataTypeConversion {

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  def main(args: Array[String]): Unit = {
    val a = 20
    val b = 15
    if(a < b) {
      //println(b + " is greater than " + a)
      println(s"$b is greater than $a")
    } else {
      //println(a + " is greater than " + b)
      println(s"$a is greater than $b")
    }

    val string1 = "abc1"
    val pattern1 = "^[a-zA-Z ]*"

    if(string1.matches(pattern1)) {
      println("String matched")
    }

    val string2 = "1003342s"
    val pattern2 = "^[0-9]*"

    if(string2.matches(pattern2)) {
      println("Integer Matched :: " + string2.toLong)
    }

    val string3 = "12314037498"
    val pattern3 = "^[0-9]*\\.[0-9]*"

    if(string3.matches(pattern3)) {
      println("Decimal Matched :: " + string3.toDouble)
    }

    val string4 = "2020-06-02"
    val pattern4 = "[0-9]{4}-[0-9]{2}-[0-9]{2}"

    if(string4.matches(pattern4)) {
      println(s"Date Format Matched :: " + LocalDate.parse(string4, formatter))
    }

    val inputString1 = "2020-05-30"
    val inputString2 = "2020-06-02"

    val result: String = (inputString1, inputString2) match {
      case (s1, s2) if inputString1.matches(pattern2) & inputString2.matches(pattern2) => (s2.toLong - s1.toLong).toString
      case (s1, s2) if inputString1.matches(pattern3) & inputString2.matches(pattern3) => (s2.toDouble - s1.toDouble).toString
      case (s1, s2) if inputString1.matches(pattern4) & inputString2.matches(pattern4) =>
        println("Inside Date Case Statements")
        val maxDate = LocalDate.parse(s2, formatter)
        val minDate = LocalDate.parse(s1, formatter)
        val diff = maxDate.toEpochDay - minDate.toEpochDay
        diff.toString
      case _ => "NULL"
    }
    println(result)
  }
}
