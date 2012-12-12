package azavea

import com.vividsolutions.jts.geom.{Point,Geometry,GeometryFactory,PrecisionModel}
import com.vividsolutions.jts.io.WKTReader

import azavea.slick.PostgisDriver.simple._
import Database.threadLocalSession


object T {
  object City extends Table[(Int,String,Geometry)]("cities") with Geo {
      
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name")
    def geom = geoColumn[Point]("geom", 4326)

    def * = id ~ name ~ geom
    def forInsert = name ~ geom
  }
  
  val db = Database.forURL("jdbc:postgresql:slick",
                                  driver="org.postgresql.Driver",
                                  user="adam",
                                  password="adam")

  def body {
    // Insert some stock data
    City.forInsert.insertAll(data:_*)

    // Grab cities in a bounding box
    val q = for {
        c <- City if c.id === 5
      } yield (c.name, c.geom)

    val (name,geom) = q.first
    println("name: " + name)
    println("geom: " + geom)

    val q2 = for {
        c <- City if c.geom.distance(geom) < 1.0
      } yield (c.name, c.geom, c.geom.distance(geom))

    println(q2.selectStatement)

    q2 foreach {
      case (name, geom, d) => println("found: %s at %s (%f degs)" format (name, geom, d))
    }

  }

  def fac() = new GeometryFactory(new PrecisionModel(), 4326)
  def wkt[T <: Geometry](s: String) = new WKTReader(fac).read(s).asInstanceOf[T]

  def go {
    db withSession {
      try {
        City.ddl.create
        body
      } finally {
        City.ddl.drop
      }
    }
  }


  def data: Array[(String, Geometry)] = 
"""[ABE]  40.65   75.43  Allentown,PA
[AOO]  40.30   78.32  Altoona,PA
[BVI]  40.75   80.33  Beaver Falls,PA
[BSI]  40.27   79.09  Blairsville,PA
[BFD]  41.80   78.63  Bradford,PA
[DUJ]  41.18   78.90  Dubois,PA
[ERI]  42.08   80.18  Erie,PA
[FKL]  41.38   79.87  Franklin,PA
[CXY]  40.22   76.85  Harrisburg,PA
[HAR]  40.37   77.42  Harrisburg,PA
[JST]  40.32   78.83  Johnstown,PA
[LNS]  40.13   76.30  Lancaster,PA
[LBE]  40.28   79.40  Latrobe,PA
[MDT]  40.20   76.77  Middletown,PA
[MUI]  40.43   76.57  Muir,PA
[PNE]  40.08   75.02  Nth Philadel,PA
[PHL]  39.88   75.25  Philadelphia,PA
[PSB]  41.47   78.13  Philipsburg,PA
[AGC]  40.35   79.93  Pittsburgh,PA
[PIT]  40.50   80.22  Pittsburgh,PA
[RDG]  40.38   75.97  Reading,PA
[43M]  39.73   77.43  Site R,PA
[UNV]  40.85   77.83  State Colleg,PA
[AVP]  41.33   75.73  Wilkes-Barre,PA
[IPT]  41.25   76.92  Williamsport,PA
[NXX]  40.20   75.15  Willow Grove,PA
""".split("\n").map(str => (str.substring(7,12), str.substring(15,20), str.substring(22))).map(_ match {
  case (lat,lng,city) =>
    (city, wkt("POINT(%f %f)" format (lng.toDouble, lat.toDouble)))
                                                                                               })
    
}



