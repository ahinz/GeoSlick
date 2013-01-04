package geoslick

import language.implicitConversions

import scala.slick.util.MacroSupport.macroSupportInterpolation

import scala.collection.mutable.ArrayBuffer

import scala.slick.SlickException
import scala.slick.lifted._
import scala.slick.ast._
import scala.slick.driver._

import scala.slick.session.{PositionedParameters, PositionedResult}

import scala.slick.driver.PostgresDriver

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.io.{WKBReader, WKBWriter, InputStreamInStream, WKTWriter}

//TODO: Where clauses should not use "As_EWKB"
trait PostgisDriver extends PostgresDriver {

  //==================================================================
  // Implicit support

  trait Implicits extends super.Implicits with PostgisConversions {
    import scala.slick.driver.BasicProfile

    implicit object GeomTypeMapper extends BaseTypeMapper[Geometry] {
      // I *don't* think this is the correct way to do this...?
      def apply(profile: BasicProfile) = profile match {
          case p: PostgisDriver => p.typeMapperDelegates.geomTypeMapperDelegate
          case _ => sys.error("only supported by the postgis driver")
        }
    }

    implicit val optGeomTypeMapper = MappedTypeMapper.base[Geometry,Option[Geometry]](
        i => if (i eq null) None else Some(i),
        o => o.getOrElse(null))
  }

  class SimpleQL extends super.SimpleQL with Implicits {
    type Postgis = PostgisDriver.Postgis
    type PostgisTable[T] = PostgisDriver.PostgisTable[T]
  }

  abstract class PostgisTable[T](schema: Option[String], table: String)
      extends Table[T](schema, table) with Postgis {
    def this(tblName: String) = this(None, tblName)
  }

  override val simple = new SimpleQL

  import simple._

  //==================================================================
  // Main mixin trait for Geo tables...
  // should this be mixed into tables automatically if you're using
  // the postgis driver?

  case class SRID(srid: Int) extends ColumnOption[Nothing]
  case class Dims(dims: Int) extends ColumnOption[Nothing]

  sealed trait GeoTypeBase[T] extends ColumnOption[T] {
    type ColumnType

    val name: String

    def asGeom = new GeoType[ColumnType](name)
    def mapper:TypeMapper[ColumnType]
  }

  class GeoType[T](val name: String) extends GeoTypeBase[T] {
    type ColumnType = Geometry
    
    def mapper = implicitly[TypeMapper[ColumnType]]
  }

  object GeoType {
    def unapply[T](gt: GeoType[T]): Option[String] = Some(gt.name)
  }

  class OptionGeoType[T](val base: GeoTypeBase[T]) extends GeoTypeBase[Option[T]] {
    type ColumnType = Option[Geometry]    
    def mapper = implicitly[TypeMapper[ColumnType]]

    val name = base.name    
  }

  trait Postgis { self: Table[_] =>
    // Take a column and wrap it with a function that is called
    // only on select. In this case, geometry fields get wrapped
    // with ST_AsEWKB on select and are inserted as raw bytes
    def wrapColumnWithSelectOnlyFunction[C](c: Column[C], f: String)(implicit tm: TypeMapper[C]) =
      new Library.SqlFunction(f) {
        override def typed(tpe: Type, ch: Node*): Apply with TypedNode =
          SelectOnlyApply(c.nodeDelegate, this, ch, tpe)
        override def typed[T : TypeMapper](ch: Node*): Apply with TypedNode =
          SelectOnlyApply(c.nodeDelegate, this, ch, implicitly[TypeMapper[T]])
       }.column(c.nodeDelegate)(tm)

    def wrapEWKB[C](c: Column[C])(implicit tm: TypeMapper[C]): Column[C] =
      wrapColumnWithSelectOnlyFunction(c, "ST_AsEWKB")

    def geoColumn[C](n: String, srid: Int)(implicit gt: GeoTypeBase[C]): Column[gt.ColumnType] = {
      val col = column[gt.ColumnType](n, gt.asGeom, SRID(srid))(gt.mapper)
      wrapEWKB(col)(gt.mapper)
    }

    @inline implicit def geoTypeToOptionGeoType[T](implicit gt: GeoTypeBase[T]): OptionGeoType[T] =
      new OptionGeoType[T](gt)

    implicit object GeometryType extends GeoType[Geometry]("GEOMETRY")

    implicit object PointType extends GeoType[Point]("POINT")
    implicit object LineStringType extends GeoType[LineString]("LINESTRING")
    implicit object PolygonType extends GeoType[Polygon]("POLYGON")

    implicit object MultiPointType extends GeoType[MultiPoint]("MULTIPOINT")
    implicit object MultiLineStringType extends GeoType[MultiLineString]("MULTILINESTRING")
    implicit object MultiPolygonType extends GeoType[MultiPolygon]("MULTIPOLYGON")
  }

  //==================================================================
  // Geometry type mapping

  override val typeMapperDelegates = new TypeMapperDelegates

  class TypeMapperDelegates extends super.TypeMapperDelegates {
    val geomTypeMapperDelegate = new GeomTypeMapperDelegate

    class GeomTypeMapperDelegate extends TypeMapperDelegate[Geometry] {
      def wkbReader = new WKBReader() // These are not threadsafe
      def wkbWriter = new WKBWriter(2, true) // so always get a fresh copy
      def wktWriter = new WKTWriter(2)

      def geomToBytes(g: Geometry): Array[Byte] =
        if (g eq null) null else wkbWriter.write(g)
      def bytesToGeom(b: Array[Byte]): Geometry =
        if (b eq null) null else wkbReader.read(b)

      def zero = null
      def sqlTypeName = "GEOMETRY"
      def sqlType = java.sql.Types.BINARY
      def setValue(v: Geometry, p: PositionedParameters) = p.setBytes(geomToBytes(v))
      def setOption(v: Option[Geometry], p: PositionedParameters) =
        if (v == None) p.setNull(sqlType) else p.setBytes(geomToBytes(v.get))
      def nextValue(r: PositionedResult) = bytesToGeom(r.nextBytes)
      def updateValue(v: Geometry, r: PositionedResult) = r.updateBytes(geomToBytes(v))
      override def valueToSQLLiteral(value: Geometry) =
        "ST_GeomFromEWKT('SRID=%d;%s')" format (value.getSRID(), wktWriter.write(value))
    }
  }

  //==================================================================
  // We have to override the insert builder to correctly
  // handle our new SelectOnlyApply node type
  //
  // Since the original version uses an inner recursive function
  // we have to yank the entire thing out

  override def createInsertBuilder(node: Node): InsertBuilder =
    new InsertBuilder(node)

  // all of this is from slick, except 'SelectOnlyApply'
  class InsertBuilder(node: Node) extends super.InsertBuilder(node) {
    override protected def buildParts(node: Node): PartsResult = {
      val cols = new ArrayBuffer[FieldSymbol]
      var table: String = null
      def f(c: Any): Unit = c match {
        case ProductNode(ch) => ch.foreach(f)
        case SelectOnlyApply(node) => f(node)
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

  trait SelectOnlyApply { self: Apply =>
    def wrappedNodeDelegate: Node

    override def toString = "SelectOnlyApply(%s)" format super.toString
  }

  object SelectOnlyApply {
    def apply(node: Node, sym: Symbol, children: Seq[Node], tp: Type): Apply with TypedNode =
      new Apply(sym, children) with SelectOnlyApply with TypedNode {
        def tpe = tp
        override protected[this] def nodeRebuild(ch: IndexedSeq[scala.slick.ast.Node]) = Apply(sym, ch, tp)
        override def nodeRebuildWithReferences(syms: IndexedSeq[Symbol]) = Apply(syms(0), children, tp)

        val wrappedNodeDelegate = node
      }

    def unapply(a: SelectOnlyApply): Option[Node] = Some(a.wrappedNodeDelegate)
  }

  //==================================================================
  // Building geometry columns is such fun

  override def createColumnDDLBuilder(column: FieldSymbol, table: Table[_]): ColumnDDLBuilder = new ColumnDDLBuilder(column)

  class ColumnDDLBuilder(column: FieldSymbol) extends super.ColumnDDLBuilder(column) {
    def isGeometryColumn = sqlType == "geometry"

    /** Seems to be issues when setting
        these explicitly to None (init order?) */
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

    // this must already exist....?
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

  //==================================================================
  // building geometry tables
  override def createTableDDLBuilder(table: Table[_]): TableDDLBuilder = new TableDDLBuilder(table)

  class TableDDLBuilder(table: Table[_]) extends super.TableDDLBuilder(table) {
    override def buildDDL: DDL =
      super.buildDDL ++ new DDL {
        val createPhase1 = createGeomColumns
        val createPhase2 = Seq()
        val dropPhase1 = Seq()
        val dropPhase2 = Seq()
      }

    //TODO: this method should be refactored
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

  //=================================================================
  // Query Building
  override def createQueryBuilder(input: QueryBuilderInput): QueryBuilder =
    new PostgisQueryBuilder(input)


  class PostgisQueryBuilder(input: QueryBuilderInput) extends QueryBuilder(input) {
    /*
     * I'm not 100% sure this is right...
     * Delete expects a tree something like:
     * Comprehension(fetch = None, offset = None)
     *  from @2110217109: Table cities
     *  select: Pure
     *    value: ProductNode
     *      1: Path @2110217109.id
     *      2: Path @2110217109.name
     *
     * When we wrap the select with a function we get something like:
     * Comprehension(fetch = None, offset = None)
     *  from @498146522: Comprehension(fetch = None, offset = None)
     *    from @348440261: Table cities
     *    select: Pure
     *      value: StructNode
     *        @1449747101: Path @348440261.id
     *        @550067645: Path @348440261.name
     *        @69809180: SelectOnlyApply(Apply Function ST_AsEWKB)
     *          0: Path @348440261.geom
     *  select: Pure
     *    value: ProductNode
     *      1: Path @498146522.@1449747101
     *      2: Path @498146522.@550067645
     *      3: Path @498146522.@69809180
     *
     * So we attempt to initially pattern match on a 'no-arg'
     * comprehension and then use some copied code from slick
     * to build the delete
     *
     * I'm not sure this approach is valid... is there a time
     * when you might a select like this?
     */

    // The inner structs end up being referenced by
    // where clauses in the outer structs. This causes
    // aliased symbols to fail so we sneak in an define
    // the symbols directly
    def evalAnnonPathsInInnerStruct(c: Node) = c match {
        case Comprehension(_, _, _, _, Some(Pure(StructNode(seq))), _, _) =>
          for( (sym,node) <- seq )
            Path.unapply(node).map { elements =>
              elements.flatMap {
                case FieldSymbol(sym) => Some(sym)
                case _ => None
              }.headOption.map { fieldName =>
                symbolName(sym) = quoteIdentifier(fieldName)
              }
            }
      }

    // Note--- we could also traverse here and remove the extra "As_EWKT" call
    // TODO: Add in a compiler phase for this? Custom selects?
    override def buildDelete: QueryBuilderResult = {
      input.ast match {
        case Comprehension(Seq((sym, c@Comprehension(Seq((_,TableNode(tbl))),_,_,_,_,_,_))),
                           where: Seq[Node],
                           None,
                           Seq(),
                           Some(Pure(ProductNode(_))),
                           None, None) => {
          evalAnnonPathsInInnerStruct(c)
          symbolName(sym) = quoteIdentifier(tbl)
          
          createDeleteSet(c, Some(where))
        }
        case o =>
          createDeleteSet(o)
      }
    }

    // this is pretty much right from slick
    def createDeleteSet(c: Node, wOpt: Option[Seq[Node]] = None) = {
      val (gen, from, innerWhere) = c match {
          case Comprehension(Seq((sym, from: TableNode)), where, _, _, Some(Pure(select)), None, None) => (sym, from, where)
          case o => throw new SlickException("A query for a DELETE statement must resolve to a comprehension with a single table -- Unsupported shape: "+o)
        }
      val where = wOpt.getOrElse(innerWhere)
      val qtn = quoteIdentifier(from.tableName)
      symbolName(gen) = qtn // Alias table to itself because DELETE does not support aliases
      b"delete from $qtn"
      if(!where.isEmpty) {
        b" where "
        expr(where.reduceLeft((a, b) => Library.And(a, b)), true)
      }
      QueryBuilderResult(b.build, input.linearizer)
    }
  }
}

object PostgisDriver extends PostgisDriver
