package azavea.slick

import language.implicitConversions

import scala.slick.ast.{Library, Node}
import scala.slick.lifted.{Column, ExtensionMethods}

import com.vividsolutions.jts.geom._
import azavea.slick.PostgisDriver._

object PostgisLibrary {

  class GeoFunction(s: String) extends Library.SqlFunction(s)

  // Geometry Relationship Functions
  val Distance = new GeoFunction("ST_Distance")
  val Equals = new GeoFunction("ST_Equals")
  val Disjoint = new GeoFunction("ST_Equals")
  val Intersects = new GeoFunction("ST_Intersects")
  val Touches = new GeoFunction("ST_Touches")
  val Crosses = new GeoFunction("ST_Crosses")
  val Within = new GeoFunction("ST_Within")
  val Overlaps = new GeoFunction("ST_Overlaps")
  val Contains = new GeoFunction("ST_Contains")
  val Covers = new GeoFunction("ST_Covers")
  val CoveredBy = new GeoFunction("ST_CoveredBy")
  val Relate = new GeoFunction("ST_Relate")
  val DWithin = new GeoFunction("ST_DWithin")

  // Geometry Processing Functions
  val Centroid = new GeoFunction("ST_Centroid")
  val Area = new GeoFunction("ST_Area")
  val Length = new GeoFunction("ST_Length")
  val PointOnSurface = new GeoFunction("ST_PointOnSurface")
  val Boundary = new GeoFunction("ST_Boundary")
  val Buffer = new GeoFunction("ST_Buffer") 
  val ConvexHull = new GeoFunction("ST_ConvexHull")
  val Intersection = new GeoFunction("ST_Intersection")
  val SymDifference = new GeoFunction("ST_SymDifference")
  val Difference = new GeoFunction("ST_Difference")
  val Union = new GeoFunction("ST_Union")
  val MemUnion = new GeoFunction("ST_MemUnion")
  val ShiftLongitude = new GeoFunction("ST_Shift_Longitude")
  
  // Geometry Accessors
  val AsText = new GeoFunction("ST_AsText")
  val SRID = new GeoFunction("ST_SRID")
  val Dimension = new GeoFunction("ST_Dimension")
  val Envelope = new GeoFunction("ST_Envelope")
  val IsEmpty = new GeoFunction("ST_IsEmpty")
  val IsSimple = new GeoFunction("ST_IsSimple")
  val IsClosed = new GeoFunction("ST_IsClosed")
  val IsRing = new GeoFunction("ST_IsRing")
  val NumGeometries = new GeoFunction("ST_NumGeometries")
  val GeometryN = new GeoFunction("ST_GeometryN")
  val NumPoints = new GeoFunction("ST_NumPoints")
  val PointN = new GeoFunction("ST_PointN")
  val ExteriorRing = new GeoFunction("ST_ExteriorRing")
  val NumInteriorRings = new GeoFunction("ST_NumInteriorRings")
  val InteriorRingN = new GeoFunction("ST_InteriorRingN")
  val EndPoint = new GeoFunction("ST_EndPoint")
  val StartPoint = new GeoFunction("ST_StartPoint")
  val GeometryType = new GeoFunction("ST_GeometryType")
  val X = new GeoFunction("ST_X")
  val Y = new GeoFunction("ST_Y")
  val Z = new GeoFunction("ST_Z")
  val M = new GeoFunction("ST_M")

  //TODO: regular "GeometryType" without ST_?

  class GeoOperator(name: String) extends Library.SqlOperator(name)

  val === = new GeoOperator("=")
  val &< = new GeoOperator("&<")
  val &> = new GeoOperator("&>")
  val << = new GeoOperator("<<")
  val >> = new GeoOperator(">>")
  val &<| = new GeoOperator("&<|")
  val |&> = new GeoOperator("|&>")
  val <<| = new GeoOperator("<<|")
  val |>> = new GeoOperator(">>|")
  val ~= = new GeoOperator("~=")
  val @@ = new GeoOperator("@")
  val ~ = new GeoOperator("~")
  val && = new GeoOperator("&&")
}

final class GeometryColumnExtensionMethods[P1](val c: Column[P1]) extends AnyVal with ExtensionMethods[Geometry, P1] {
  def distance[P2,R](e: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Double, R]) =
    om(PostgisLibrary.Distance.column(n, Node(e)))

  def gequals[P2,R](e: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) =
    om(PostgisLibrary.Equals.column(n, Node(e)))    

  def disjoint[P2,R](e: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) =
    om(PostgisLibrary.Disjoint.column(n, Node(e)))    

  def intersects[P2,R](e: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) =
    om(PostgisLibrary.Intersects.column(n, Node(e)))    

  def touches[P2,R](e: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) =
    om(PostgisLibrary.Touches.column(n, Node(e)))    

  def crosses[P2,R](e: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) =
    om(PostgisLibrary.Crosses.column(n, Node(e)))    

  def overlaps[P2,R](e: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) =
    om(PostgisLibrary.Overlaps.column(n, Node(e)))    

  def contains[P2,R](e: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) =
    om(PostgisLibrary.Contains.column(n, Node(e)))    

  def covers[P2,R](e: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) =
    om(PostgisLibrary.Covers.column(n, Node(e)))    

  def coveredBy[P2,R](e: Column[P2])(implicit om: o#arg[Geometry, P2]#to[Boolean, R]) =
    om(PostgisLibrary.CoveredBy.column(n, Node(e)))    

  def relate[P2,R](e: Column[P2])(implicit om: o#arg[Geometry, P2]#to[String, R]) =
    om(PostgisLibrary.Relate.column(n, Node(e)))    

  def dwithin[P2,R](e: Column[P2])(implicit om: o#arg[Geometry, P2]#arg[Double,P2]#to[Boolean, R]) =
    om(PostgisLibrary.DWithin.column(n, Node(e)))

  // processing

  def centroid[P2, R](e: Column[P2])(implicit om: o#to[Geometry, R]) =
    om(PostgisLibrary.Centroid.column(n, Node(e)))    

  def area[P2, R](e: Column[P2])(implicit om: o#to[Double, R]) =
    om(PostgisLibrary.Area.column(n, Node(e)))    

  def length[P2, R](e: Column[P2])(implicit om: o#to[Double, R]) =
    om(PostgisLibrary.Length.column(n, Node(e)))    

  def pointOnSurface[P2, R](e: Column[P2])(implicit om: o#to[Geometry, R]) =
    om(PostgisLibrary.PointOnSurface.column(n, Node(e)))

  def convexHull[P2,R](e: Column[P2])(implicit om: o#to[Geometry, R]) =
    om(PostgisLibrary.ConvexHull.column(n, Node(e)))    

  def intersection[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Geometry, R]) =
    om(PostgisLibrary.Intersection.column(n, Node(e)))    

  def symDifference[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Geometry, R]) =
    om(PostgisLibrary.SymDifference.column(n, Node(e)))    

  def Difference[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Geometry, R]) =
    om(PostgisLibrary.Difference.column(n, Node(e)))    

  def Union[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Geometry, R]) =
    om(PostgisLibrary.Union.column(n, Node(e)))    

  def MemUnion[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Geometry, R]) =
    om(PostgisLibrary.MemUnion.column(n, Node(e)))    

  def ShiftLongitude[P2,R](e: Column[P2])(implicit om: o#to[Geometry, R]) =
    om(PostgisLibrary.ShiftLongitude.column(n, Node(e)))    

  def Buffer[P2,R](e: Column[P2])(implicit om: o#arg[Int,P2]#to[Geometry, R]) =
    om(PostgisLibrary.Buffer.column(n, Node(e)))    

  // Accessors
  def AsText[P2,R](e: Column[P2])(implicit om: o#to[String, R]) =
    om(PostgisLibrary.AsText.column(n, Node(e)))    

  def SRID[P2,R](e: Column[P2])(implicit om: o#to[Int, R]) =
    om(PostgisLibrary.SRID.column(n, Node(e)))    

  def Dimension[P2,R](e: Column[P2])(implicit om: o#to[Int, R]) =
    om(PostgisLibrary.Dimension.column(n, Node(e)))    

  def Envelope[P2,R](e: Column[P2])(implicit om: o#to[Geometry, R]) =
    om(PostgisLibrary.Envelope.column(n, Node(e)))    

  def IsEmpty[P2,R](e: Column[P2])(implicit om: o#to[Boolean, R]) =
    om(PostgisLibrary.IsEmpty.column(n, Node(e)))    

  def IsSimple[P2,R](e: Column[P2])(implicit om: o#to[Boolean, R]) =
    om(PostgisLibrary.IsSimple.column(n, Node(e)))    

  def IsClosed[P2,R](e: Column[P2])(implicit om: o#to[Boolean, R]) =
    om(PostgisLibrary.IsClosed.column(n, Node(e)))    

  def IsRing[P2,R](e: Column[P2])(implicit om: o#to[Boolean, R]) =
    om(PostgisLibrary.IsRing.column(n, Node(e)))    

  def NumGeometries[P2,R](e: Column[P2])(implicit om: o#to[Int, R]) =
    om(PostgisLibrary.NumGeometries.column(n, Node(e)))    

  def GeometryN[P2,R](e: Column[P2])(implicit om: o#arg[Int,P2]#to[Geometry, R]) =
    om(PostgisLibrary.GeometryN.column(n, Node(e)))    

  def NumPoints[P2,R](e: Column[P2])(implicit om: o#to[Int, R]) =
    om(PostgisLibrary.NumPoints.column(n, Node(e)))    

  def PointN[P2,R](e: Column[P2])(implicit om: o#arg[Int,P2]#to[Geometry, R]) =
    om(PostgisLibrary.PointN.column(n, Node(e)))    

  def ExteriorRing[P2,R](e: Column[P2])(implicit om: o#arg[Int,P2]#to[Geometry, R]) =
    om(PostgisLibrary.PointN.column(n, Node(e)))    

  def NumInteriorRings[P2,R](e: Column[P2])(implicit om: o#to[Int, R]) =
    om(PostgisLibrary.NumInteriorRings.column(n, Node(e)))    

  def InteriorRingN[P2,R](e: Column[P2])(implicit om: o#arg[Int,P2]#to[Geometry, R]) =
    om(PostgisLibrary.InteriorRingN.column(n, Node(e)))    

  def EndPoint[P2,R](e: Column[P2])(implicit om: o#to[Geometry, R]) =
    om(PostgisLibrary.EndPoint.column(n, Node(e)))    

  def StartPoint[P2,R](e: Column[P2])(implicit om: o#to[Geometry, R]) =
    om(PostgisLibrary.StartPoint.column(n, Node(e)))    

  def GeometryType[P2,R](e: Column[P2])(implicit om: o#to[String, R]) =
    om(PostgisLibrary.GeometryType.column(n, Node(e)))    

  def X[P2,R](e: Column[P2])(implicit om: o#to[Double, R]) =
    om(PostgisLibrary.X.column(n, Node(e)))    

  def Y[P2,R](e: Column[P2])(implicit om: o#to[Double, R]) =
    om(PostgisLibrary.Y.column(n, Node(e)))    

  def Z[P2,R](e: Column[P2])(implicit om: o#to[Double, R]) =
    om(PostgisLibrary.Z.column(n, Node(e)))    

  def M[P2,R](e: Column[P2])(implicit om: o#to[Double, R]) =
    om(PostgisLibrary.M.column(n, Node(e)))    

  // Operators
  def ===[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Boolean, R]) =
    om(PostgisLibrary.===.column(n, Node(e)))

  def &<[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Boolean,R]) =
    om(PostgisLibrary.&<.column(n, Node(e)))

  def &>[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Boolean,R]) =
    om(PostgisLibrary.&>.column(n, Node(e)))

  def <<[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Boolean,R]) =
    om(PostgisLibrary.<<.column(n, Node(e)))

  def >>[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Boolean,R]) =
    om(PostgisLibrary.>>.column(n, Node(e)))

  def &<|[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Boolean,R]) =
    om(PostgisLibrary.&<|.column(n, Node(e)))

  def |&>[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Boolean,R]) =
    om(PostgisLibrary.|&>.column(n, Node(e)))

  def <<|[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Boolean,R]) =
    om(PostgisLibrary.<<|.column(n, Node(e)))

  def |>>[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Boolean,R]) =
    om(PostgisLibrary.|>>.column(n, Node(e)))

  def ~=[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Boolean,R]) =
    om(PostgisLibrary.~=.column(n, Node(e)))

  def @@[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Boolean,R]) =
    om(PostgisLibrary.@@.column(n, Node(e)))

  def ~[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Boolean,R]) =
    om(PostgisLibrary.~.column(n, Node(e)))

  def &&[P2,R](e: Column[P2])(implicit om: o#arg[Geometry,P2]#to[Boolean,R]) =
    om(PostgisLibrary.&&.column(n, Node(e)))
}

trait PostgisConversions {
  implicit def geometryColumnExtensionMethods(c: Column[Geometry]) = new GeometryColumnExtensionMethods[Geometry](c)
  implicit def geometryColumnOptionExtensionMethods(c: Column[Option[Geometry]]) = new GeometryColumnExtensionMethods[Option[Geometry]](c)
}
