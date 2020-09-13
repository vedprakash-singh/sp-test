import scala.annotation.tailrec

object GeneralCoding extends App {

  val test = List(
    Record("Rajesh", 21, "London"),
    Record("Suresh", 28, "California"),
    Record("Sam", 26, "Delhi"),
    Record("Rajesh", 21, "Gurgaon"),
    Record("Manish", 29, "Bengaluru")
  )

  def getDistinct(xs: List[Record]) = {
    @tailrec def inner(remaining: List[Record], acc: List[Record]): List[Record] = {
      if (remaining.isEmpty) acc
      else if (acc.exists(r => r.name == remaining.head.name && r.age == remaining.head.age)) inner(remaining.tail, acc)
      else inner(remaining.tail, remaining.head :: acc)
    }

    inner(xs, List.empty[Record])
  }

  case class Record(name: String, age: Int, location: String)

  println(getDistinct(test))
}



