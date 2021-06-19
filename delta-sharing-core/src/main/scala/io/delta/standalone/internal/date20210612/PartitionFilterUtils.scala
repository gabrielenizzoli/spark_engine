package io.delta.standalone.internal.date20210612

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, caseInsensitiveResolution}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BindReferences, Cast, EqualNullSafe, EqualTo, Expression, ExtractValue, GreaterThan, GreaterThanOrEqual, InterpretedPredicate, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Not}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.slf4j.LoggerFactory

import scala.util.Try
import scala.util.control.NonFatal

object PartitionFilterUtils {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val sqlParser = new SparkSqlParser()

  def evaluatePredicate(
                         schemaString: String,
                         partitionColumns: Seq[String],
                         partitionFilters: Seq[String],
                         addFiles: Seq[AddFile]): Seq[AddFile] = {
    try {
      val tableSchema = DataType.fromJson(schemaString).asInstanceOf[StructType]
      val partitionSchema = new StructType(partitionColumns.map(c => tableSchema(c)).toArray)
      val addSchema = Encoders.product[AddFile].schema
      val attrs =
        addSchema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
      val exprs =
        rewritePartitionFilters(
          partitionSchema,
          attrs,
          partitionFilters.flatMap { f =>
            Try(sqlParser.parseExpression(f)).toOption
          }.filter(f => isSupportedExpression(f, partitionSchema))
        )
      if (exprs.isEmpty) {
        addFiles
      } else {
        val predicate = new InterpretedPredicate(BindReferences.bindReference(exprs.reduce(And), attrs))
        predicate.initialize(0)
        addFiles.filter { addFile =>
          val converter = CatalystTypeConverters.createToCatalystConverter(addSchema)
          predicate.eval(converter(addFile).asInstanceOf[InternalRow])
        }
      }
    } catch {
      case NonFatal(e) =>
        logger.error(e.getMessage, e)
        // Fail to evaluate the filters. Return all files as a fallback.
        addFiles
    }
  }

  private def isSupportedExpression(e: Expression, partitionSchema: StructType): Boolean = {
    def isPartitionColumOrConstant(e: Expression): Boolean = {
      e match {
        case _: Literal => true
        case u: UnresolvedAttribute if u.nameParts.size == 1 =>
          val unquoted = u.name.stripPrefix("`").stripSuffix("`")
          partitionSchema.exists(part => caseInsensitiveResolution(unquoted, part.name))
        case c: Cast => isPartitionColumOrConstant(c.child)
        case _ => false
      }
    }

    e match {
      case EqualTo(left, right)
        if isPartitionColumOrConstant(left) && isPartitionColumOrConstant(right) =>
        true
      case GreaterThan(left, right)
        if isPartitionColumOrConstant(left) && isPartitionColumOrConstant(right) =>
        true
      case LessThan(left, right)
        if isPartitionColumOrConstant(left) && isPartitionColumOrConstant(right) =>
        true
      case GreaterThanOrEqual(left, right)
        if isPartitionColumOrConstant(left) && isPartitionColumOrConstant(right) =>
        true
      case LessThanOrEqual(left, right)
        if isPartitionColumOrConstant(left) && isPartitionColumOrConstant(right) =>
        true
      case EqualNullSafe(left, right)
        if isPartitionColumOrConstant(left) && isPartitionColumOrConstant(right) =>
        true
      case IsNull(e) if isPartitionColumOrConstant(e) =>
        true
      case IsNotNull(e) if isPartitionColumOrConstant(e) =>
        true
      case Not(e) if isSupportedExpression(e, partitionSchema) =>
        true
      case _ => false
    }
  }

  private def rewritePartitionFilters(
                                       partitionSchema: StructType,
                                       attrs: Seq[Attribute],
                                       partitionFilters: Seq[Expression]): Seq[Expression] = {
    val partitionValuesAttr = attrs.find(_.name == "partitionValues").head
    partitionFilters.map(_.transformUp {
      case a: Attribute =>
        // If we have a special column name, e.g. `a.a`, then an UnresolvedAttribute returns
        // the column name as '`a.a`' instead of 'a.a', therefore we need to strip the backticks.
        val unquoted = a.name.stripPrefix("`").stripSuffix("`")
        val partitionCol = partitionSchema.find { field => field.name == unquoted }
        partitionCol match {
          case Some(StructField(name, dataType, _, _)) =>
            Cast(
              ExtractValue(
                partitionValuesAttr,
                Literal(name),
                org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution),
              dataType)
          case None =>
            // This should not be able to happen, but the case was present in the original code so
            // we kept it to be safe.
            UnresolvedAttribute(Seq("partitionValues", a.name))
        }
    })
  }
}
