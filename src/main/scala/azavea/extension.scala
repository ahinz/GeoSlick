package azavea.slick

import scala.slick.ast._
import scala.slick.lifted._

import com.vividsolutions.jts.geom._
import azavea.slick.PostgisDriver._
import azavea.slick.PostgisDriver.Implicit._


object PostgisLibrary {
  val Distance = new Library.SqlFunction("ST_Distance")
}

final class GeometryColumnExtensionMethods[P1](val c: Column[P1]) extends AnyVal with ExtensionMethods[Geometry, P1] {
  def distance[P2,R](e: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Double, R]) =
    om(PostgisLibrary.Distance.column(n, Node(e)))
}

//type arg[B1, P1] = {
//    type to[BR, PR] = OptionMapper2[B1, B1, BR, P1, P1, PR]
// getOptionMapper2TT[B1, B2 : BaseTypeMapper, BR] = 
//     OptionMapper2.plain.asInstanceOf[OptionMapper2[B1,B2,BR,B1,B2,BR]]

trait PostgisConversions {
  implicit def geometryColumnExtensionMethods(c: Column[Geometry]) = new GeometryColumnExtensionMethods[Geometry](c)
  implicit def geometryColumnOptionExtensionMethods(c: Column[Option[Geometry]]) = new GeometryColumnExtensionMethods[Option[Geometry]](c)
}
