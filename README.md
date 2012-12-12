## GeoSlick

### Overview

GeoSlick is an experimental library that adds support for PostGIS
to Slick

### Supported Features

Pretty much all PostGIS functions are supported

### Example

```scala
import azavea.slick.PostgisDriver.simple._
import Database.threadLocalSession

object City extends Table[(Int,String,Geometry)]("cities") with Geo {
      
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def name = column[String]("name")
  def geom = geoColumn[Point]("geom", 4326)

  def * = id ~ name ~ geom
}

def fac() = new GeometryFactory(new PrecisionModel(), 4326)
def wkt[T <: Geometry](s: String) = new WKTReader(fac).read(s).asInstanceOf[T]

Database.forURL("jdbc:postgresql:slick",
                driver="org.postgresql.Driver",
                user="user", password="adam") withSession {

  City.ddl.create

  City.insert(1,"Philadelphia","POINT(-75.25 39.88)")
  City.insert(2,"Pittsburgh","POINT(-79.93 40.35)")

  val (n,g) = (for { c <- City if c.id === 2 } yield (c.name,c.geom)).first

  val q2 = for {
    c <- City if c.geom.distance(g) < 1.2
  } yield (c.name, c.geom, c.geom.distance(g))

  println(q2.selectStatement)

  q2 foreach {
    case (name, geom, d) => println("found: %s at %s (%f degs)" format (name, geom, d))
  }

  City.ddl.drop
               
}
```

Note that `c.geom.distance(g)` is turned into a PostGIS function. The select
statement for q2 looks something like:

```sql
select x2."name", 
       ST_AsEWKB(x2."geom"), 
       ST_Distance(ST_AsEWKB(x2."geom"),
         ST_GeomFromEWKT('SRID=4326;POINT(-79.93 40.35)'))
from "cities" x2 
where ST_Distance(ST_AsEWKB(x2."geom"),
        ST_GeomFromEWKT('SRID=4326;POINT(-79.93 40.35)')) < 1.2
```
