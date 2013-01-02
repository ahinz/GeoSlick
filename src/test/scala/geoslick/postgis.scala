package geoslick

import org.scalatest._

import PostgisDriver.simple._
import Database.threadLocalSession

import com.vividsolutions.jts.geom.{Point,Geometry,GeometryFactory,PrecisionModel}
import com.vividsolutions.jts.io.WKTReader

import util._

class PostgisSpec extends FlatSpec with ShouldMatchers {

  object SimpleCity extends PostgisTable[(Int,String)]("cities") {
    
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name")

    def * = id ~ name
    def forInsert = name
  }

  object City extends PostgisTable[(Int,String,Geometry)]("cities") {
      
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name")
    def geom = geoColumn[Point]("geom", 4326)

    def * = id ~ name ~ geom
    def forInsert = name ~ geom
  }

  val pguser = scala.util.Properties.envOrElse("PGUSER","postgres")
  val pgpass = scala.util.Properties.envOrElse("PGPASS","postgres")
  val pgdb = scala.util.Properties.envOrElse("PGDB","slick")
  val pghost = scala.util.Properties.envOrElse("PGHOST","localhost:5432")

  val db = Database.forURL("jdbc:postgresql://" + pghost + "/" + pgdb,
                           driver="org.postgresql.Driver",
                           user=pguser,
                           password=pgpass)

  "Environment" should "be sane" in {    
    db withSession {
      // Make sure things are clean
      // we probably shouldn't need this
      try { SimpleCity.ddl.drop } catch { case e: Throwable =>  }

      val cities = Seq("washington","london","paris")

      SimpleCity.ddl.create
      SimpleCity.forInsert.insertAll(cities:_*)

      val q = for { c <- SimpleCity } yield c.name

      q.list should equal (cities)

      SimpleCity.ddl.drop
    }
  }

  "Postgis driver" should "be able to insert geoms" in {
    db withSession {
      // Make sure things are clean
      // we probably shouldn't need this
      try { City.ddl.drop } catch { case e: Throwable =>  }

      City.ddl.create
      City.forInsert.insertAll(data:_*)

      val q = for { c <- City } yield (c.name, c.geom)

      q.list should equal (data.toList)
                                  
      City.ddl.drop
    }
  }

  it should "be able to query with geo fcns" in {
    db withSession {
      // Make sure things are clean
      // we probably shouldn't need this
      try { City.ddl.drop } catch { case e: Throwable =>  }

      City.ddl.create
      City.forInsert.insertAll(data:_*)

      // 40.30, 78.32 -> Altoona,PA
      val bbox = bboxBuffer(78.32, 40.30, 0.01)
      
      // Operator
      val q = for {
          c <- City if c.geom && bbox // && -> overlaps
        } yield c.name

      q.list should equal (List("Altoona,PA"))
      
      // Function
      val dist = 0.5
      val q2 = for {
          c1 <- City
          c2 <- City if c1.geom.distance(c2.geom) < dist && c1.name =!= c2.name
        } yield (c1.name, c2.name, c1.geom.distance(c2.geom))

      val q2format = q2.list map {
          case (n1,n2,d) => (n1,n2,"%1.4f" format d)
        }

      val jts = for {
          j1 <- data
          j2 <- data if j1._2.distance(j2._2) < dist && j1._1 != j2._1
        } yield (j1._1, j2._1, "%1.4f" format j1._2.distance(j2._2))

      q2format should equal (jts.toList)
          
      // Output function
      val q3 = for {
          c <- City if c.name === "Reading,PA"
        } yield c.geom.asGeoJson()

      q3.first should equal ("""{"type":"Point","coordinates":[76,40.4]}""")

      City.ddl.drop
    }
  }

  object OptCity extends PostgisTable[(Int,String,Option[Geometry])]("cities") {
      
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name")
    def geom = geoColumn[Option[Point]]("geom", 4326)

    def * = id ~ name ~ geom
    def forInsert = name ~ geom
  }

  it should "be able to handle optional fields" in {
    db withSession {
      try { OptCity.ddl.drop } catch { case e: Throwable => }

      OptCity.ddl.create

      val cities = Seq(("washington",Some(pt(-77.02,38.53))),
                       ("london", None),
                       ("paris", Some(pt(2.3470,48.8742))))

      OptCity.forInsert.insertAll(cities:_*)

      val q1 = for {
          c <- OptCity if c.geom isNull
        } yield (c.name, c.geom)

      q1.list should equal (List(("london",None)))

      val q2 = for {
          c <- OptCity if c.geom isNotNull
        } yield c.name

      q2.list should equal (List("washington","paris"))

      OptCity.ddl.drop
    }
  }

  it should "be able to query with geo fcns on null fields" in {
    db withSession {
      // Make sure things are clean
      // we probably shouldn't need this
      try { OptCity.ddl.drop } catch { case e: Throwable =>  }

      val data2 = data.map {
          case (s,g) => (s, Some(g))
        }

      OptCity.ddl.create
      OptCity.forInsert.insertAll(data2:_*)

      // 40.30, 78.32 -> Altoona,PA
      val bbox = bboxBuffer(78.32, 40.30, 0.01)
      
      val q = for {
          c <- OptCity if c.geom && bbox // && -> overlaps
        } yield c.name

      q.list should equal (List("Altoona,PA"))

      OptCity.ddl.drop
    }
  }

}
