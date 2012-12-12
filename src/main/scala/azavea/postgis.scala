package azavea.slick

import scala.collection.mutable.ArrayBuffer
import scala.slick.ast._

import scala.slick.SlickException
import scala.slick.lifted._
import scala.slick.driver._
import scala.slick.ast.FieldSymbol

import java.io.InputStream
import java.sql.Blob
import scala.slick.session.{PositionedParameters, PositionedResult}

import scala.slick.driver.PostgresDriver
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.io.{WKBReader, WKBWriter, InputStreamInStream, WKTWriter}

trait PostgisDriver extends PostgresDriver {  
  case class SRID(srid: Int) extends ColumnOption[Nothing]
  case class Dims(srid: Int) extends ColumnOption[Nothing]
  case class GeoType[T <: Geometry](s: String) extends ColumnOption[Nothing]

  trait Implicits extends super.Implicits with PostgisConversions {
    implicit object GeomTypeMapper extends BaseTypeMapper[Geometry] {
      def apply(profile: BasicProfile) = profile match {
          case p: PostgisDriver => p.typeMapperDelegates.geomTypeMapperDelegate
          case _ => sys.error("only supported by the postgis driver")
        }
    }
  }

  class SimpleQL extends super.SimpleQL with Implicits {
    type Geo = PostgisDriver.Geo
  }

  override val simple = new SimpleQL

  import simple._
  
  case class SelectOnlyApply(s: Node, fname: String) extends Library.SqlFunction(fname) {
    override def toString = "SelectOnlyApply(%s)" format super.toString

    override def typed[T : TypeMapper](ch: Node*): SelectOnlyApplyAux =
      new SelectOnlyApplyAux(s, this, ch, implicitly[TypeMapper[T]])
  }

  class SelectOnlyApplyAux(val select: Node, s: Symbol, kids: Seq[Node], tp: Type) extends Apply(s, kids) with TypedNode {
    def tpe = tp
    override protected[this] def nodeRebuild(ch: IndexedSeq[scala.slick.ast.Node]) = Apply(sym, ch, tp)
    override def nodeRebuildWithReferences(syms: IndexedSeq[Symbol]) = Apply(syms(0), children, tp)

    override def toString = "SelectOnlyApplyAux(%s)" format super.toString
  }
  
  object SelectOnlyApplyAux {
    def unapply(s: SelectOnlyApplyAux):Option[Node] = Some(s.select)
  }

  trait Geo { self: Table[_] =>
    def wrapEWKB(c: Column[Geometry]):Column[Geometry] =
      SelectOnlyApply(c.nodeDelegate, "ST_AsEWKB").column(c.nodeDelegate)

    def geoColumn[T <: Geometry](
      colName: String, srid: Int)(implicit geo: GeoType[T]):Column[Geometry] =
      wrapEWKB(column[Geometry](colName, SRID(srid), geo))

    implicit object PointType extends GeoType[Point]("POINT")
    implicit object LineStringType extends GeoType[LineString]("LINESTRING")
    implicit object PolygonType extends GeoType[Polygon]("POLYGON")

    implicit object MultiPointType extends GeoType[MultiPoint]("MULTIPOINT")
    implicit object MultiLineStringType extends GeoType[MultiLineString]("MULTILINESTRING")
    implicit object MultiPolygonType extends GeoType[MultiPolygon]("MULTIPOLYGON")
  }

  override val typeMapperDelegates = new TypeMapperDelegates

  class TypeMapperDelegates extends super.TypeMapperDelegates {
    val geomTypeMapperDelegate = new GeomTypeMapperDelegate

    class GeomTypeMapperDelegate extends TypeMapperDelegate[Geometry] {
      def reader = new WKBReader() // These are not threadsafe
      def writer = new WKBWriter(2,true) // so always get a fresh copy
      def twriter = new WKTWriter(2)
      def asInStream(i: InputStream) = new InputStreamInStream(i)
      def readGeomFromByteArr(b: Array[Byte]) = reader.read(b)

      def geomAsByteArr(g: Geometry):Array[Byte] = 
        writer.write(g)

      def sqlTypeName = "geometry"
      def zero = null
      def sqlType = java.sql.Types.BLOB
      def setValue(v: Geometry, p: PositionedParameters) =
        p.setBytes(geomAsByteArr(v))

      def setOption(v: Option[Geometry], p: PositionedParameters) =
        p.setBytesOption(v.map(geomAsByteArr _))

      def nextValue(r: PositionedResult) = readGeomFromByteArr(r.nextBytes)
      def updateValue(v: Geometry, r: PositionedResult) = r.updateBytes(geomAsByteArr(v))

      override def valueToSQLLiteral(value: Geometry) =
        "ST_GeomFromEWKT('SRID=%d;%s')" format (value.getSRID(), twriter.write(value))
    }
  }

  override def createInsertBuilder(node: Node): InsertBuilder =
    new InsertBuilder(node)

  class InsertBuilder(node: Node) extends super.InsertBuilder(node) {
    override protected def buildParts(node: Node): PartsResult = {
      val cols = new ArrayBuffer[FieldSymbol]
      var table: String = null
      def f(c: Any): Unit = c match {
        case ProductNode(ch) => ch.foreach(f)
        case SelectOnlyApplyAux(s) => f(s)
        case t:TableNode => f(Node(t.nodeShaped_*.value))
        case Select(Ref(IntrinsicSymbol(t: TableNode)), field: FieldSymbol) =>
          if(table eq null) table = t.tableName
          else if(table != t.tableName) throw new SlickException("Inserts must all be to the same table")
          cols += field
        }
      f(node)
      if(table eq null) throw new SlickException("No table to insert into")
      new PartsResult(table, cols)
    }
  }

  override def createColumnDDLBuilder(column: FieldSymbol, table: Table[_]): ColumnDDLBuilder = new ColumnDDLBuilder(column)

  override def createTableDDLBuilder(table: Table[_]): TableDDLBuilder = new TableDDLBuilder(table)

  class ColumnDDLBuilder(column: FieldSymbol) extends super.ColumnDDLBuilder(column) {
    def isGeometryColumn = sqlType == "geometry"

    var srid: Option[Int] = _
    var dims: Option[Int] = _
    var geoType: Option[String] = _

    if (srid == null) srid = None
    if (dims == null) dims = None
    if (geoType == null) geoType = None

    override protected def handleColumnOption(o: ColumnOption[_]): Unit = o match {
        case SRID(s) => srid = Some(s)
        case Dims(d) => dims = Some(d)
        case GeoType(s) => geoType = Some(s)
        case _ => super.handleColumnOption(o)
      }
   
    def getAddGeometry(tbl: String):Option[String] = {
      if (isGeometryColumn) {
        val sb = new StringBuilder
        val s = srid.getOrElse(sys.error("Missing required option: SRID"))
        val geomType = geoType.getOrElse(sys.error("Missing required geo type"))
        val d = dims.getOrElse(2)
        sb append "SELECT AddGeometryColumn("
        //sb append quoteIdentifier("public") append ","
        sb append quoteString(tbl) append ","
        sb append quoteString(column.name) append ","
        sb append s append ","
        sb append quoteString(geomType) append ","
        sb append d append ")"
        Some(sb.toString)
      } else {
        None
      }
    }

    def quoteString(s: String) = "'%s'" format s

    override def appendColumn(sb: StringBuilder) {
      if (!isGeometryColumn) {
        sb append quoteIdentifier(column.name) append ' '
        if(autoIncrement) {
          sb append "SERIAL"
          autoIncrement = false
        }
        else sb append sqlType
        appendOptions(sb)
      } 
    }
  }

  class TableDDLBuilder(table: Table[_]) extends super.TableDDLBuilder(table) {
    override def buildDDL: DDL = {
      if(primaryKeys.size > 1)
        throw new SlickException("Table "+table.tableName+" defines multiple primary keys ("
          + primaryKeys.map(_.name).mkString(", ") + ")")
      new DDL {
        val createPhase1 = Iterable(createTable) ++ primaryKeys.map(createPrimaryKey) ++ indexes.map(createIndex) ++ createGeomColumns
        val createPhase2 = foreignKeys.map(createForeignKey)
        val dropPhase1 = foreignKeys.map(dropForeignKey)
        val dropPhase2 = primaryKeys.map(dropPrimaryKey) ++ Iterable(dropTable)
      }
    }

    override protected def createTable: String = {
      val b = new StringBuilder append "create table " append quoteIdentifier(table.tableName) append " ("
      for(n <- table.create_*) {
        val cb = createColumnDDLBuilder(n, table)
        if (!cb.isGeometryColumn) {
          cb.appendColumn(b)
          b append ","
        }
      }
      //TODO: This is a horrible hack...
      b.deleteCharAt(b.length-1)
      addTableOptions(b)
      b append ")"
      b.toString
    }

    def createGeomColumn(f: FieldSymbol):Option[String] =
      createColumnDDLBuilder(f, table).getAddGeometry(table.tableName)

    def createGeomColumns: Iterable[String] = 
      table.create_*.flatMap(createGeomColumn _)
      
  }
}

object PostgisDriver extends PostgisDriver 
