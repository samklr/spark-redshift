/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.spark_redshift_community.spark.redshift.pushdown.querygeneration

import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project, SubqueryAlias, View}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class EliminateViewSuite extends AnyFunSuite {

  private val child: LogicalPlan = LocalRelation(
    AttributeReference("id", IntegerType)(),
    AttributeReference("name", StringType)()
  )

  private val catalogTable = CatalogTable(
    identifier = TableIdentifier("test_view"),
    tableType = CatalogTableType.VIEW,
    storage = CatalogStorageFormat.empty,
    schema = new StructType()
  )

  private val viewPlan: LogicalPlan = View(catalogTable, isTempView = false, child)

  private val queryBuilder = new QueryBuilder(child)

  // Verifies that type-based View matching extracts the child plan correctly.
  test("View type match extracts child via .child accessor") {
    viewPlan match {
      case v: View =>
        assert(v.child eq child)
      case _ =>
        fail("Should match a View plan")
    }
  }

  // Verifies that a non-View plan does not match the View type.
  test("Non-View plan does not match View type") {
    child match {
      case _: View =>
        fail("LocalRelation should not match View type")
      case _ =>
        succeed
    }
  }

  // Verifies that temp views are also matched, since isTempView does not
  // affect the type or the .child accessor.
  test("Temp view matches View type") {
    val tempView = View(catalogTable, isTempView = true, child)
    tempView match {
      case v: View =>
        assert(v.child eq child)
      case _ =>
        fail("Should match a temp View")
    }
  }

  // Verifies that the View type match works regardless of the CatalogTable
  // schema content, ensuring it is agnostic to metadata fields.
  test("View with non-empty schema matches View type") {
    val schemaTable = CatalogTable(
      identifier = TableIdentifier("schema_view"),
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("name", StringType)
      ))
    )
    val viewWithSchema = View(schemaTable, isTempView = false, child)
    viewWithSchema match {
      case v: View =>
        assert(v.child eq child)
      case _ =>
        fail("Should match a View regardless of schema")
    }
  }

  // Verifies that SubqueryAlias does not match the View type, since it is
  // a different wrapper node handled by a separate case in the elimination logic.
  test("SubqueryAlias does not match View type") {
    val subqueryAlias = SubqueryAlias("alias", child)
    subqueryAlias match {
      case _: View =>
        fail("SubqueryAlias should not match View type")
      case _ =>
        succeed
    }
  }

  // Verifies that Project does not match the View type.
  test("Project does not match View type") {
    val project = Project(Seq.empty, child)
    project match {
      case _: View =>
        fail("Project should not match View type")
      case _ =>
        succeed
    }
  }

  // Verifies the full recursive elimination pattern used in
  // QueryBuilder.EliminateSubqueryAliasesAndView: nested Views and
  // SubqueryAliases are all stripped to yield the innermost child plan.
  test("Recursive elimination strips nested Views and SubqueryAliases") {
    val innerView = View(catalogTable, isTempView = false, child)
    val outerView = View(catalogTable, isTempView = true, innerView)
    val wrappedPlan = SubqueryAlias("alias", outerView)

    val result = queryBuilder.EliminateSubqueryAliasesAndView(wrappedPlan)
    assert(result eq child)
  }

}
